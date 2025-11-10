package vfs

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/puzpuzpuz/xsync/v4"
	"github.com/sirrobot01/decypharr/pkg/manager"
	"github.com/sirrobot01/decypharr/pkg/mount/dfs/vfs/ranges"
)

// Buffer pool for streaming downloads (128KB buffers)
var downloadBufferPool = sync.Pool{
	New: func() interface{} {
		buf := make([]byte, 128*1024) // 128KB
		return &buf
	},
}

// downloadJob tracks an active chunk download with progressive updates
type downloadJob struct {
	chunkStart int64
	chunkSize  int64
	done       chan error
	mu         sync.Mutex
	completed  bool

	// Progressive download support
	bytesDownloaded atomic.Int64  // How much has been downloaded so far
	minThreshold    int64         // Minimum bytes before early return
	thresholdReady  chan struct{} // Signals when minThreshold is reached
	thresholdOnce   sync.Once     // Ensures threshold signal sent only once
}

// File represents a cached remote file with integrated download capabilities
// Combines sparse file tracking, download logic, and prefetching in a single efficient struct
type File struct {
	// File metadata
	path      string
	info      *manager.FileInfo
	chunkSize int64

	// On-disk file and range tracking
	file       *os.File
	ranges     *ranges.Ranges // Lazy-loaded from metadata
	rangesOnce sync.Once      // Ensures ranges are loaded exactly once

	// Memory buffer (ultra-fast memory-first caching)
	memBuffer *MemoryBuffer

	// Download configuration
	readAhead int64

	// Active downloads tracking
	downloading *xsync.Map[int64, *downloadJob]

	// Stats and lifecycle
	stats           *StatsTracker
	manager         *manager.Manager // Reference to save metadata
	vfsManager      *Manager         // Reference to VFS manager
	lastAccess      atomic.Int64
	lastReadOffset  atomic.Int64 // Track last read offset for sequential detection
	modTime         time.Time
	dirty           bool // Has unflushed changes to metadata
	bytesDownloaded atomic.Int64

	// Lifecycle management
	mu        sync.RWMutex
	closeOnce sync.Once
	closeChan chan struct{} // Signals all goroutines to stop
	closed    atomic.Bool   // Indicates if file is closed
}

// newFile creates or opens a cached file with download capabilities
func newFile(cacheDir string, info *manager.FileInfo, chunkSize, readAhead int64, stats *StatsTracker, manager *manager.Manager, vfsManager *Manager) (*File, error) {
	sanitizedFileName := sanitizeForPath(info.Name())
	torrentDir := filepath.Join(cacheDir, sanitizeForPath(info.Parent()))
	cachePath := filepath.Join(torrentDir, sanitizedFileName)

	// Ensure directory exists
	if err := os.MkdirAll(torrentDir, 0755); err != nil {
		return nil, fmt.Errorf("create cache dir: %w", err)
	}

	// Open or create file
	file, err := os.OpenFile(cachePath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, fmt.Errorf("open cache file: %w", err)
	}

	// Set file size (sparse allocation)
	if err := file.Truncate(info.Size()); err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("truncate cache file: %w", err)
	}

	// Create memory buffer for ultra-fast access
	// Use context that will be cancelled when file closes
	bufferCtx := context.Background()
	memBufferSize := vfsManager.config.BufferSize
	if memBufferSize <= 0 {
		memBufferSize = 64 * 1024 * 1024 // Default 64MB if not configured
	}
	memBuffer := NewMemoryBuffer(bufferCtx, memBufferSize, chunkSize)
	memBuffer.AttachFile(file) // Attach for async flushing

	f := &File{
		path:        cachePath,
		info:        info,
		chunkSize:   chunkSize,
		file:        file,
		ranges:      nil, // Lazy-loaded via rangesOnce
		memBuffer:   memBuffer,
		readAhead:   readAhead,
		downloading: xsync.NewMap[int64, *downloadJob](),
		stats:       stats,
		manager:     manager,
		vfsManager:  vfsManager,
		modTime:     info.ModTime(),
		dirty:       false,
		closeChan:   make(chan struct{}),
	}

	f.lastAccess.Store(time.Now().UnixNano())

	// Initialize last read offset to -1 (no previous read)
	f.lastReadOffset.Store(-1)

	return f, nil
}

func (f *File) touchAccess() {
	f.lastAccess.Store(time.Now().UnixNano())
}

func (f *File) lastAccessTime() time.Time {
	ts := f.lastAccess.Load()
	if ts == 0 {
		return time.Time{}
	}
	return time.Unix(0, ts)
}

// ensureFileOpen lazily (re)opens the on-disk sparse file if it was closed.
func (f *File) ensureFileOpen() error {
	f.mu.RLock()
	if f.file != nil {
		f.mu.RUnlock()
		return nil
	}
	f.mu.RUnlock()

	f.mu.Lock()
	defer f.mu.Unlock()

	if f.file != nil {
		return nil
	}

	file, err := os.OpenFile(f.path, os.O_RDWR, 0644)
	if err != nil {
		return fmt.Errorf("reopen cache file: %w", err)
	}
	f.file = file

	if f.memBuffer != nil {
		f.memBuffer.AttachFile(file)
	}

	return nil
}

// ensureRangesLoaded ensures ranges are loaded exactly once (thread-safe)
func (f *File) ensureRangesLoaded() {
	f.rangesOnce.Do(func() {
		f.ranges = ranges.New()
		if !f.loadCachedMetadata() {
			_ = f.scanExistingData()
		}
	})
}

func (f *File) loadCachedMetadata() bool {
	if f.vfsManager == nil {
		return false
	}

	meta, err := f.vfsManager.loadMetadata(f.info.Parent(), f.info.Name())
	if err != nil || meta == nil {
		return false
	}

	for _, r := range meta.Ranges {
		f.ranges.Insert(r)
	}

	if !meta.ATime.IsZero() {
		f.lastAccess.Store(meta.ATime.UnixNano())
	}
	if !meta.ModTime.IsZero() {
		f.modTime = meta.ModTime
	}

	var cached int64
	for _, r := range meta.Ranges {
		cached += r.Size
	}
	if cached > 0 {
		f.bytesDownloaded.Store(cached)
	}

	f.mu.Lock()
	f.dirty = meta.Dirty
	f.mu.Unlock()

	return true
}

// scanExistingData scans the file to detect which chunks already have data
// This is a fallback when metadata doesn't exist
func (f *File) scanExistingData() error {
	// Read file in chunks and check if they contain non-zero data
	// This is a heuristic - we check a few bytes per chunk for performance

	numChunks := (f.info.Size() + f.chunkSize - 1) / f.chunkSize
	sample := make([]byte, 4096) // Sample first 4KB of each chunk

	for chunkIdx := int64(0); chunkIdx < numChunks; chunkIdx++ {
		offset := chunkIdx * f.chunkSize
		readSize := int64(len(sample))
		if offset+readSize > f.info.Size() {
			readSize = f.info.Size() - offset
		}

		n, err := f.file.ReadAt(sample[:readSize], offset)
		if err != nil && n == 0 {
			continue // Chunk likely not downloaded
		}

		// Check if sample contains non-zero bytes
		hasData := false
		for i := 0; i < n; i++ {
			if sample[i] != 0 {
				hasData = true
				break
			}
		}

		if hasData {
			// Mark entire chunk as present
			chunkEnd := offset + f.chunkSize
			if chunkEnd > f.info.Size() {
				chunkEnd = f.info.Size()
			}
			f.ranges.Insert(ranges.Range{
				Pos:  offset,
				Size: chunkEnd - offset,
			})
		}
	}

	return nil
}

// readAt reads from cache if data is available, returns false if not cached
// Supports partial reads: if only part of the requested range is cached, returns what's available
func (f *File) readAt(p []byte, offset int64) (n int, cached bool, err error) {
	// Ensure ranges are loaded exactly once (no lock upgrade needed!)
	f.ensureRangesLoaded()

	if err := f.ensureFileOpen(); err != nil {
		return 0, false, err
	}

	f.mu.RLock()
	defer f.mu.RUnlock()

	f.touchAccess()

	requestedRange := ranges.Range{Pos: offset, Size: int64(len(p))}

	// Check if requested range is fully cached
	if f.ranges.Present(requestedRange) {
		// Fully cached - read everything
		n, err = f.file.ReadAt(p, offset)
		if err != nil {
			return n, false, err
		}
		return n, true, nil
	}

	// Not fully cached - check if we can do a partial read
	// Find what gaps exist in the requested range
	missing := f.ranges.FindMissing(requestedRange)
	if len(missing) == 0 {
		// Shouldn't happen (Present() returned false but no missing ranges)
		// Treat as fully cached
		n, err = f.file.ReadAt(p, offset)
		if err != nil {
			return n, false, err
		}
		return n, true, nil
	}

	// Check if there's cached data at the beginning of the request
	firstMissing := missing[0]
	if firstMissing.Pos > offset {
		// We have cached data from offset to firstMissing.Pos
		availableSize := firstMissing.Pos - offset
		n, err = f.file.ReadAt(p[:availableSize], offset)
		if err != nil && err != io.EOF {
			return n, false, err
		}
		return n, true, nil // Partial read success
	}

	// No cached data at the start of the requested range
	return 0, false, nil
}

// WriteAt writes data and updates the range map
func (f *File) WriteAt(p []byte, offset int64) (int, error) {
	// Ensure ranges are loaded exactly once (no lock upgrade needed!)
	f.ensureRangesLoaded()

	if err := f.ensureFileOpen(); err != nil {
		return 0, err
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	// Write to file
	n, err := f.file.WriteAt(p, offset)
	if err != nil {
		return n, err
	}

	// Mark this range as cached
	if n > 0 {
		f.ranges.Insert(ranges.Range{
			Pos:  offset,
			Size: int64(n),
		})
		f.dirty = true
	}

	f.touchAccess()
	return n, nil
}

// Sync flushes data to disk
func (f *File) Sync() error {
	if err := f.ensureFileOpen(); err != nil {
		return err
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	// Sync file data
	if f.file != nil {
		return f.file.Sync()
	}

	return nil
}

// closeFD closes the file descriptor
func (f *File) closeFD() error {
	if f.memBuffer != nil {
		_ = f.memBuffer.Flush()
		f.memBuffer.AttachFile(nil)
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	if f.file != nil {
		err := f.file.Close()
		f.file = nil
		return err
	}
	return nil
}

// Close closes the file and stops all downloads
func (f *File) Close() error {
	var firstErr error

	if err := f.persistMetadata(true); err != nil {
		firstErr = err
	}

	_ = f.CloseDownloads()

	if err := f.closeFD(); err != nil && firstErr == nil {
		firstErr = err
	}

	if f.memBuffer != nil {
		if err := f.memBuffer.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}

	return firstErr
}

// removeFromDisk removes the sparse file from disk
func (f *File) removeFromDisk() error {
	f.mu.Lock()
	defer f.mu.Unlock()

	// Close file if open
	if f.file != nil {
		_ = f.file.Close()
		f.file = nil
	}

	// Remove sparse file
	if err := os.Remove(f.path); err != nil && !os.IsNotExist(err) {
		return err
	}

	return nil
}

func (f *File) persistMetadata(force bool) error {
	if f.vfsManager == nil {
		return nil
	}

	f.ensureRangesLoaded()

	f.mu.RLock()
	dirty := f.dirty
	modTime := f.modTime
	f.mu.RUnlock()

	if !dirty && !force {
		return nil
	}

	meta := &Metadata{
		ModTime: modTime,
		ATime:   f.lastAccessTime(),
		Size:    f.info.Size(),
		Ranges:  f.ranges.GetRanges(),
		Dirty:   false,
	}

	if meta.ModTime.IsZero() {
		meta.ModTime = time.Now()
	}
	if meta.ATime.IsZero() {
		meta.ATime = time.Now()
	}

	if err := f.vfsManager.saveMetadata(f.info.Parent(), f.info.Name(), meta); err != nil {
		return err
	}

	f.mu.Lock()
	f.dirty = false
	f.mu.Unlock()

	return nil
}

// GetCachedSize returns the total bytes downloaded
func (f *File) GetCachedSize() int64 {
	// Ensure ranges are loaded exactly once (no lock upgrade needed!)
	f.ensureRangesLoaded()

	f.mu.RLock()
	defer f.mu.RUnlock()

	if f.ranges == nil {
		return 0
	}
	return f.ranges.Size()
}

// GetCachedRanges returns all cached ranges (for debugging/stats)
func (f *File) GetCachedRanges() []ranges.Range {
	// Ensure ranges are loaded exactly once (no lock upgrade needed!)
	f.ensureRangesLoaded()

	f.mu.RLock()
	defer f.mu.RUnlock()

	if f.ranges == nil {
		return nil
	}
	return f.ranges.GetRanges()
}

// FindMissing returns ranges that need to be downloaded
func (f *File) FindMissing(offset, length int64) []ranges.Range {
	// Ensure ranges are loaded exactly once (no lock upgrade needed!)
	f.ensureRangesLoaded()

	f.mu.RLock()
	defer f.mu.RUnlock()

	if f.ranges == nil {
		// If ranges failed to load, assume everything is missing
		return []ranges.Range{{Pos: offset, Size: length}}
	}

	return f.ranges.FindMissing(ranges.Range{
		Pos:  offset,
		Size: length,
	})
}

// IsCached checks if a range is cached WITHOUT allocating buffers
// This is much more efficient than readAt for cache checks
func (f *File) IsCached(offset, length int64) bool {
	// Ensure ranges are loaded exactly once (no lock upgrade needed!)
	f.ensureRangesLoaded()

	f.mu.RLock()
	defer f.mu.RUnlock()

	if f.ranges == nil {
		return false
	}

	return f.ranges.Present(ranges.Range{
		Pos:  offset,
		Size: length,
	})
}

// ReadAt reads data with progressive download for instant playback start
// Uses ring buffer streaming for large sequential reads (instant playback)
// Falls back to chunk-based caching for random/small reads
func (f *File) ReadAt(ctx context.Context, p []byte, offset int64) (int, error) {
	size := f.info.Size()
	if offset >= size {
		return 0, io.EOF
	}

	readSize := int64(len(p))
	if offset+readSize > size {
		readSize = size - offset
		p = p[:readSize]
	}

	f.touchAccess()

	// FAST PATH: Check memory buffer first (< 1ms)
	if f.memBuffer != nil {
		data, found := f.memBuffer.Get(offset, readSize)
		if found {
			// Memory hit! Ultra-fast return
			n := copy(p, data)
			f.lastReadOffset.Store(offset + int64(n))
			// Trigger prefetch in background
			go f.aggressiveSequentialPrefetch(ctx, offset+readSize)
			return n, nil
		}
	}

	// Check if fully cached on disk (zero-allocation check)
	if f.IsCached(offset, readSize) {
		// Disk cache hit - read and store in memory for next time
		n, _, err := f.readAt(p, offset)
		if err == nil {
			// Store in memory buffer for future fast access
			if f.memBuffer != nil && n > 0 {
				_ = f.memBuffer.Put(offset, p[:n])
			}
			// Update last read offset for sequential detection
			f.lastReadOffset.Store(offset + int64(n))
			go f.aggressiveSequentialPrefetch(ctx, offset+readSize)
		}
		return n, err
	}

	// Cache miss - need to download
	// Track active read
	f.stats.TrackActiveRead(1)
	defer f.stats.TrackActiveRead(-1)

	// ALWAYS use streaming for network reads - provides instant playback
	// StreamingReader efficiently handles ALL read patterns:
	// - Sequential reads (video): Instant startup, continuous streaming
	// - Random reads (seeks): Returns first chunk immediately
	// - Small reads (metadata): Minimal overhead, quick return
	n, err := f.streamingReadAt(ctx, p, offset)
	if err == nil {
		// Store in memory buffer for future fast access
		if f.memBuffer != nil && n > 0 {
			_ = f.memBuffer.Put(offset, p[:n])
		}
		// Update last read offset
		f.lastReadOffset.Store(offset + int64(n))

		// Start background prefetch for smooth playback
		go f.aggressiveSequentialPrefetch(ctx, offset+readSize)
	}
	return n, err
}

// aggressiveSequentialPrefetch downloads chunks ahead for smooth playback
func (f *File) aggressiveSequentialPrefetch(ctx context.Context, currentOffset int64) {
	if f.closed.Load() {
		return
	}

	// Download 4-6 chunks ahead for smooth playback
	numChunks := int64(6)
	if f.readAhead > 0 {
		numChunks = (f.readAhead + f.chunkSize - 1) / f.chunkSize
		if numChunks < 4 {
			numChunks = 4 // Minimum 4 chunks for smooth playback
		}
	}

	startChunk := currentOffset / f.chunkSize
	endChunk := startChunk + numChunks
	totalChunks := (f.info.Size() + f.chunkSize - 1) / f.chunkSize
	if endChunk > totalChunks {
		endChunk = totalChunks
	}

	// Download chunks concurrently
	for chunkIdx := startChunk; chunkIdx < endChunk; chunkIdx++ {
		select {
		case <-f.closeChan:
			return
		default:
			go f.downloadChunkAsync(ctx, chunkIdx)
		}
	}
}

// downloadChunkAsync downloads a chunk in background (non-blocking)
func (f *File) downloadChunkAsync(ctx context.Context, chunkIdx int64) {
	// Check if file is closed
	if f.closed.Load() {
		return
	}

	// Check if already cached (zero-allocation check)
	chunkStart := chunkIdx * f.chunkSize
	chunkEnd := chunkStart + f.chunkSize
	if chunkEnd > f.info.Size() {
		chunkEnd = f.info.Size()
	}
	if f.IsCached(chunkStart, chunkEnd-chunkStart) {
		return // Already cached
	}

	// Check if already downloading
	if _, exists := f.downloading.Load(chunkIdx); exists {
		return // Already downloading
	}

	// Start download
	_ = f.downloadChunk(ctx, chunkIdx)
}

// downloadChunk downloads a specific chunk (blocking) using fixed chunk size
func (f *File) downloadChunk(ctx context.Context, chunkIdx int64) error {
	return f.downloadChunkWithThreshold(ctx, chunkIdx, 0) // No early return
}

// downloadChunkWithThreshold downloads a chunk with optional early return
// If minThreshold > 0, returns as soon as that many bytes are available
// Background download continues to completion
func (f *File) downloadChunkWithThreshold(ctx context.Context, chunkIdx int64, minThreshold int64) error {
	chunkStart := chunkIdx * f.chunkSize
	chunkEnd := chunkStart + f.chunkSize
	if chunkEnd > f.info.Size() {
		chunkEnd = f.info.Size()
	}
	actualSize := chunkEnd - chunkStart

	// Check if already cached (zero-allocation check)
	if f.IsCached(chunkStart, actualSize) {
		return nil // Already cached
	}

	// If threshold requested, check if we have at least that much cached
	if minThreshold > 0 {
		// Check partial cache
		missing := f.FindMissing(chunkStart, actualSize)
		if len(missing) == 0 {
			return nil // Fully cached
		}

		// Calculate how much we already have
		cachedBytes := actualSize
		for _, m := range missing {
			cachedBytes -= m.Size
		}

		if cachedBytes >= minThreshold {
			// We already have enough cached data
			// Start background download for the rest if not already downloading
			if _, exists := f.downloading.Load(chunkIdx); !exists {
				go func() {
					_ = f.downloadChunk(ctx, chunkIdx)
				}()
			}
			return nil
		}
	}

	// Check if already downloading - if so, wait for it
	if job, exists := f.downloading.Load(chunkIdx); exists {
		// If threshold specified, wait for threshold, otherwise wait for completion
		if minThreshold > 0 && job.thresholdReady != nil {
			select {
			case <-job.thresholdReady:
				return nil // Threshold reached, return early
			case err := <-job.done:
				return err // Download completed or failed
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return <-job.done // Wait for existing download to complete
	}

	// Create download job with progressive support
	job := &downloadJob{
		chunkStart:     chunkStart,
		chunkSize:      actualSize,
		done:           make(chan error, 1),
		minThreshold:   minThreshold,
		thresholdReady: make(chan struct{}),
	}

	// Try to store job (race condition safe)
	actual, loaded := f.downloading.LoadOrStore(chunkIdx, job)
	if loaded {
		// Someone else started downloading
		if minThreshold > 0 && actual.thresholdReady != nil {
			select {
			case <-actual.thresholdReady:
				return nil // Threshold reached
			case err := <-actual.done:
				return err // Download completed or failed
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return <-actual.done
	}

	// Start background download
	go func() {
		err := f.doDownloadProgressive(ctx, job)
		job.mu.Lock()
		job.completed = true
		job.mu.Unlock()
		job.done <- err
		close(job.done)

		// Delete immediately after completion
		f.downloading.Delete(chunkIdx)

		if err == nil {
			f.bytesDownloaded.Add(actualSize)
		}
	}()

	// Wait for threshold or completion
	if minThreshold > 0 {
		select {
		case <-job.thresholdReady:
			return nil // Threshold reached, return early
		case err := <-job.done:
			return err // Download completed or failed before threshold
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	// No threshold, wait for completion
	return <-job.done
}

// doDownloadProgressive performs progressive download with threshold signaling
func (f *File) doDownloadProgressive(ctx context.Context, job *downloadJob) error {
	return f.doDownloadWithJob(ctx, job.chunkStart, job.chunkSize, job)
}

// doDownloadWithJob performs the actual HTTP download with optional progressive signaling
func (f *File) doDownloadWithJob(ctx context.Context, offset, size int64, job *downloadJob) error {
	end := offset + size - 1
	rc, err := f.manager.StreamReader(ctx, f.info.Parent(), f.info.Name(), offset, end)
	if err != nil {
		return fmt.Errorf("get download link: %w", err)
	}
	defer func(rc io.ReadCloser) {
		_ = rc.Close()
	}(rc)

	// Get buffer from pool (128KB)
	bufPtr := downloadBufferPool.Get().(*[]byte)
	buffer := *bufPtr
	defer downloadBufferPool.Put(bufPtr)

	// Allocate write buffer (512KB) - batch multiple reads before writing
	// This reduces lock acquisitions from 64 to 16 per 8MB chunk
	const writeBatchSize = 512 * 1024 // 512KB
	writeBuf := make([]byte, 0, writeBatchSize)
	currentOffset := offset
	totalRead := int64(0)
	thresholdSignaled := false

	for totalRead < size {
		// Check if file was closed
		if f.closed.Load() {
			return fmt.Errorf("file closed during download")
		}

		// Read from stream into pooled buffer
		n, err := rc.Read(buffer)
		if n > 0 {
			// Append to write buffer
			remaining := size - totalRead
			toWrite := int64(n)
			if toWrite > remaining {
				toWrite = remaining
			}

			writeBuf = append(writeBuf, buffer[:toWrite]...)

			// Write in batches to reduce lock contention
			if len(writeBuf) >= writeBatchSize || totalRead+toWrite >= size || err == io.EOF {
				written, writeErr := f.WriteAt(writeBuf, currentOffset)
				if writeErr != nil {
					return fmt.Errorf("write to file at offset %d: %w", currentOffset, writeErr)
				}

				currentOffset += int64(written)
				totalRead += int64(written)
				writeBuf = writeBuf[:0] // Reset buffer but keep capacity

				// Progressive download: signal when threshold reached
				if job != nil && !thresholdSignaled {
					job.bytesDownloaded.Store(totalRead)
					if totalRead >= job.minThreshold {
						job.thresholdOnce.Do(func() {
							close(job.thresholdReady)
						})
						thresholdSignaled = true
					}
				}
			}
		}

		if err == io.EOF {
			break
		}
		if err != nil {
			// Partial data already written incrementally above
			// Return error so caller knows download is incomplete
			return fmt.Errorf("read chunk at offset %d (downloaded %d/%d bytes): %w",
				currentOffset, totalRead, size, err)
		}
	}

	// Ensure threshold is signaled even if we completed before reaching it
	if job != nil && !thresholdSignaled {
		job.bytesDownloaded.Store(totalRead)
		job.thresholdOnce.Do(func() {
			close(job.thresholdReady)
		})
	}

	return nil
}

// CloseDownloads stops all download goroutines
func (f *File) CloseDownloads() error {
	// Signal all goroutines to stop (only once)
	f.closeOnce.Do(func() {
		f.closed.Store(true)
		close(f.closeChan)
	})
	return nil
}

// Stats returns download statistics
func (f *File) Stats() map[string]interface{} {
	return map[string]interface{}{
		"bytes_downloaded": f.bytesDownloaded.Load(),
		"active_downloads": f.downloading.Size(),
		"chunk_size":       f.chunkSize,
		"read_ahead":       f.readAhead,
		"cached_size":      f.GetCachedSize(),
		"total_size":       f.info.Size(),
	}
}
