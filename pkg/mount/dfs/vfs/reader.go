package vfs

import (
	"context"
	"fmt"
	"io"
	"sync"
	"sync/atomic"

	"github.com/puzpuzpuz/xsync/v4"
	"github.com/sirrobot01/decypharr/pkg/debrid/store"
	"github.com/sirrobot01/decypharr/pkg/debrid/types"
)

// Buffer pool to reuse chunk buffers and avoid excessive allocations
var chunkBufferPool = sync.Pool{
	New: func() interface{} {
		// Create 8MB buffer (typical chunk size)
		buf := make([]byte, 8*1024*1024)
		return &buf
	},
}

// Reader downloads multiple chunks concurrently
type Reader struct {
	size        int64
	debrid      *store.Cache
	sparseFile  *SparseFile
	torrentName string
	fileName    string
	fileLink    string

	chunkSize     int64 // Size of each chunk (e.g., 8MB)
	readAhead     int64 // Read-ahead size (e.g., 12MB)
	maxConcurrent int   // Max concurrent downloads (e.g., 4)

	// Active downloads tracking
	downloading *xsync.Map[int64, *downloadJob] // map[int64]*downloadJob

	// Semaphore to limit concurrent downloads
	//downloadSem chan struct{}

	// Stats
	bytesDownloaded atomic.Int64
	stats           *StatsTracker // Stats tracker for network/read operations

	// Initial prefetch state
	initialPrefetchDone atomic.Bool

	// Smart caching support
	manager *Manager // Reference to VFS manager for smart caching

	// Access pattern detection (to avoid slowing down ffprobe)
	lastReadOffset   atomic.Int64
	sequentialReads  atomic.Int64 // Count of sequential reads
	randomSeeks      atomic.Int64 // Count of random seeks
	totalReads       atomic.Int64
	isLikelyFFProbe  atomic.Bool // True if access pattern looks like ffprobe
	memoryBuffer     []byte      // In-memory buffer for first chunk
	memoryBufferSize int64
	bufferMu         sync.RWMutex
}

type downloadJob struct {
	chunkStart int64
	chunkSize  int64
	done       chan error
	mu         sync.Mutex
	completed  bool
}

// NewReader creates a reader with parallel chunk downloading
func NewReader(debridCache *store.Cache, torrentName string, torrentFile types.File, sparseFile *SparseFile, chunkSize, readAhead int64, maxConcurrent int, stats *StatsTracker, manager *Manager) *Reader {
	// Determine buffer size
	var bufferSize int64
	if manager != nil && manager.config.BufferSize > 0 {
		bufferSize = manager.config.BufferSize
	} else {
		bufferSize = 4 * 1024 * 1024 // Default 4MB
	}

	// Allocate in-memory buffer for fast access
	var memBuffer []byte
	if bufferSize > 0 && bufferSize <= torrentFile.Size {
		memBuffer = make([]byte, bufferSize)
	}

	reader := &Reader{
		size:             torrentFile.Size,
		torrentName:      torrentName,
		fileName:         torrentFile.Name,
		fileLink:         torrentFile.Link,
		debrid:           debridCache,
		sparseFile:       sparseFile,
		chunkSize:        chunkSize,
		readAhead:        readAhead,
		maxConcurrent:    maxConcurrent,
		downloading:      xsync.NewMap[int64, *downloadJob](),
		stats:            stats,
		manager:          manager,
		memoryBuffer:     memBuffer,
		memoryBufferSize: bufferSize,
		//downloadSem:   make(chan struct{}, maxConcurrent),
	}
	reader.lastReadOffset.Store(-1) // Initialize to -1 to detect first read
	return reader
}

func (pr *Reader) getDownloadLink() (types.DownloadLink, error) {
	// Check if we already have a final URL cached
	downloadLink, err := pr.debrid.GetDownloadLink(pr.torrentName, pr.fileName, pr.fileLink)
	if err != nil {
		return downloadLink, err
	}
	err = downloadLink.Valid()
	if err != nil {
		return types.DownloadLink{}, err
	}
	return downloadLink, nil
}

// ReadAt reads data with parallel chunk downloading
func (pr *Reader) ReadAt(ctx context.Context, p []byte, offset int64) (int, error) {
	if offset >= pr.size {
		return 0, io.EOF
	}

	readSize := int64(len(p))
	if offset+readSize > pr.size {
		readSize = pr.size - offset
		p = p[:readSize]
	}

	// Track access pattern to detect ffprobe
	pr.detectAccessPattern(offset, readSize)

	// Try memory buffer first (super fast for beginning of file)
	if pr.memoryBuffer != nil && offset < pr.memoryBufferSize {
		n, ok := pr.readFromMemoryBuffer(p, offset)
		if ok {
			return n, nil
		}
	}

	// Try sparseFile cache
	n, cached, err := pr.sparseFile.ReadAt(p, offset)
	if cached && err == nil {
		// Cache hit - trigger read-ahead
		// Only trigger aggressive read-ahead for sequential playback, not ffprobe
		if !pr.isLikelyFFProbe.Load() {
			go pr.scheduleReadAhead(ctx, offset+readSize)
		}
		return n, nil
	}

	// Aggressive initial prefetch ONLY for sequential playback, not ffprobe
	if !pr.initialPrefetchDone.Load() && !pr.isLikelyFFProbe.Load() && offset < pr.chunkSize*2 {
		pr.initialPrefetchDone.Store(true)
		// Aggressively prefetch first few chunks in background for fast playback
		go pr.aggressiveInitialPrefetch(ctx)
	}

	// Cache miss - need to download
	// Calculate which chunks we need
	startChunk := offset / pr.chunkSize
	endChunk := (offset + readSize - 1) / pr.chunkSize

	// Track active read
	pr.stats.TrackActiveRead(1)
	defer pr.stats.TrackActiveRead(-1)

	// Download required chunks (maybe multiple if read spans chunks)
	for chunkIdx := startChunk; chunkIdx <= endChunk; chunkIdx++ {
		if err := pr.downloadChunk(ctx, chunkIdx); err != nil {
			return 0, err
		}
	}

	// Read from sparseFile now
	n, _, err = pr.sparseFile.ReadAt(p, offset)
	if err != nil {
		return n, err
	}

	// Fill memory buffer if this is from the beginning
	if pr.memoryBuffer != nil && offset < pr.memoryBufferSize && !pr.isLikelyFFProbe.Load() {
		pr.fillMemoryBuffer(ctx, offset, n)
	}

	// Trigger read-ahead for next chunks (only for sequential playback)
	if !pr.isLikelyFFProbe.Load() {
		go pr.scheduleReadAhead(ctx, offset+readSize)
	}

	// Track file access for smart caching (episode detection)
	if pr.manager != nil && pr.manager.config.SmartCaching && !pr.isLikelyFFProbe.Load() {
		torrent := pr.debrid.GetTorrentByName(pr.torrentName)
		if torrent != nil {
			// Convert map to slice for tracking
			allFiles := make([]types.File, 0, len(torrent.Files))
			for _, file := range torrent.Files {
				allFiles = append(allFiles, file)
			}
			go pr.manager.TrackFileAccess(pr.torrentName, pr.fileName, offset+readSize, pr.size, allFiles)
		}
	}

	return n, nil
}

// detectAccessPattern detects if access pattern looks like ffprobe
func (pr *Reader) detectAccessPattern(offset, size int64) {
	pr.totalReads.Add(1)
	lastOffset := pr.lastReadOffset.Load()

	// Detect random seeks (characteristic of ffprobe)
	if lastOffset >= 0 {
		diff := offset - lastOffset
		// If jumping around (not sequential), it's likely ffprobe
		if diff < 0 || diff > pr.chunkSize*2 {
			pr.randomSeeks.Add(1)
		} else {
			pr.sequentialReads.Add(1)
		}

		// FFProbe characteristics:
		// 1. Multiple random seeks (jumping to beginning and end)
		// 2. Small read sizes (typically < 64KB)
		// 3. Reading from end of file early
		totalReads := pr.totalReads.Load()
		if totalReads >= 3 {
			randomRatio := float64(pr.randomSeeks.Load()) / float64(totalReads)
			isSmallReads := size < 65536               // 64KB
			isReadingEnd := offset > pr.size-1024*1024 // Last 1MB

			// If more than 50% random seeks OR small reads at the end, likely ffprobe
			if randomRatio > 0.5 || (isSmallReads && isReadingEnd && totalReads < 10) {
				pr.isLikelyFFProbe.Store(true)
			}
		}
	}

	pr.lastReadOffset.Store(offset + size)
}

// readFromMemoryBuffer reads from the in-memory buffer
func (pr *Reader) readFromMemoryBuffer(p []byte, offset int64) (int, bool) {
	pr.bufferMu.RLock()
	defer pr.bufferMu.RUnlock()

	// Check if buffer has this data (buffer is filled from offset 0)
	if offset >= pr.memoryBufferSize {
		return 0, false
	}

	// Check if buffer is populated (non-zero check)
	if pr.memoryBuffer[0] == 0 && pr.memoryBuffer[pr.memoryBufferSize-1] == 0 {
		return 0, false // Buffer not filled yet
	}

	// Read from buffer
	end := offset + int64(len(p))
	if end > pr.memoryBufferSize {
		end = pr.memoryBufferSize
	}

	n := copy(p, pr.memoryBuffer[offset:end])
	return n, true
}

// fillMemoryBuffer fills the memory buffer with downloaded data
func (pr *Reader) fillMemoryBuffer(ctx context.Context, offset int64, length int) {
	// Only fill from the beginning
	if offset > 0 {
		return
	}

	pr.bufferMu.Lock()
	defer pr.bufferMu.Unlock()

	// Fill buffer from sparse file
	endOffset := pr.memoryBufferSize
	if endOffset > pr.size {
		endOffset = pr.size
	}

	// Read from sparse file into memory buffer
	_, _, _ = pr.sparseFile.ReadAt(pr.memoryBuffer[:endOffset], 0)
}

// aggressiveInitialPrefetch prefetches the first chunks aggressively for fast playback start
func (pr *Reader) aggressiveInitialPrefetch(ctx context.Context) {
	// Prefetch first 3-4 chunks immediately for instant playback
	numInitialChunks := int64(4)
	if pr.readAhead > 0 {
		numInitialChunks = (pr.readAhead + pr.chunkSize - 1) / pr.chunkSize
	}

	// Don't exceed file size
	totalChunks := (pr.size + pr.chunkSize - 1) / pr.chunkSize
	if numInitialChunks > totalChunks {
		numInitialChunks = totalChunks
	}

	// Start downloading first chunks concurrently
	for chunkIdx := int64(0); chunkIdx < numInitialChunks; chunkIdx++ {
		go pr.downloadChunkAsync(ctx, chunkIdx)
	}
}

// scheduleReadAhead schedules read-ahead chunks for download
func (pr *Reader) scheduleReadAhead(ctx context.Context, fromOffset int64) {
	// Calculate read-ahead range
	readAheadEnd := fromOffset + pr.readAhead
	if readAheadEnd > pr.size {
		readAheadEnd = pr.size
	}

	startChunk := fromOffset / pr.chunkSize
	endChunk := (readAheadEnd - 1) / pr.chunkSize

	// Schedule chunks for background download
	for chunkIdx := startChunk; chunkIdx <= endChunk; chunkIdx++ {
		go pr.downloadChunkAsync(ctx, chunkIdx)
	}
}

// downloadChunkAsync downloads a chunk in background (non-blocking)
func (pr *Reader) downloadChunkAsync(ctx context.Context, chunkIdx int64) {
	// Check if already cached (no allocation!)
	chunkStart := chunkIdx * pr.chunkSize
	chunkEnd := chunkStart + pr.chunkSize
	if chunkEnd > pr.size {
		chunkEnd = pr.size
	}

	if pr.sparseFile.IsCached(chunkStart, chunkEnd-chunkStart) {
		return // Already cached
	}

	// Check if already downloading
	if _, exists := pr.downloading.Load(chunkIdx); exists {
		return // Already downloading
	}

	// Start download
	_ = pr.downloadChunk(ctx, chunkIdx)
}

// downloadChunk downloads a specific chunk (blocking)
func (pr *Reader) downloadChunk(ctx context.Context, chunkIdx int64) error {
	chunkStart := chunkIdx * pr.chunkSize
	chunkEnd := chunkStart + pr.chunkSize
	if chunkEnd > pr.size {
		chunkEnd = pr.size
	}
	chunkSize := chunkEnd - chunkStart

	// Check sparseFile first (no allocation!)
	if pr.sparseFile.IsCached(chunkStart, chunkSize) {
		return nil // Already cached
	}

	// Check if already downloading - if so, wait for it
	if job, exists := pr.downloading.Load(chunkIdx); exists {
		return <-job.done // Wait for existing download
	}

	// Create download job
	job := &downloadJob{
		chunkStart: chunkStart,
		chunkSize:  chunkSize,
		done:       make(chan error, 1),
	}

	// Try to store job (race condition safe)
	actual, loaded := pr.downloading.LoadOrStore(chunkIdx, job)
	if loaded {
		// Someone else started downloading, wait for them
		return <-actual.done
	}

	go func() {
		// Acquire semaphore to limit concurrent downloads
		//pr.downloadSem <- struct{}{}
		//defer func() { <-pr.downloadSem }()
		err := pr.doDownload(ctx, chunkStart, chunkSize)
		job.mu.Lock()
		job.completed = true
		job.mu.Unlock()
		job.done <- err
		close(job.done)

		// Delete immediately after completion
		pr.downloading.Delete(chunkIdx)

		if err == nil {
			pr.bytesDownloaded.Add(chunkSize)
		}
	}()

	// Wait for download to complete
	return <-job.done
}

// doDownload performs the actual HTTP download
func (pr *Reader) doDownload(ctx context.Context, offset, size int64) error {
	end := offset + size - 1
	rc, err := pr.debrid.StreamReader(ctx, offset, end, pr.getDownloadLink)
	if err != nil {
		return fmt.Errorf("get download link: %w", err)
	}
	defer func(rc io.ReadCloser) {
		_ = rc.Close()
	}(rc)

	// Get buffer from pool (reuse memory!)
	bufPtr := chunkBufferPool.Get().(*[]byte)
	defer chunkBufferPool.Put(bufPtr) // Return to pool when done

	buffer := (*bufPtr)[:size] // Slice to exact size needed
	totalRead := 0

	for totalRead < int(size) {
		n, err := rc.Read(buffer[totalRead:])
		totalRead += n

		if err == io.EOF {
			break
		}
		if err != nil {
			// Write partial data
			if totalRead > 0 {
				_, _ = pr.sparseFile.WriteAt(buffer[:totalRead], offset)
			}
			return fmt.Errorf("read chunk: %w", err)
		}
	}

	// Write to sparseFile
	if totalRead > 0 {
		_, err := pr.sparseFile.WriteAt(buffer[:totalRead], offset)
		if err != nil {
			return fmt.Errorf("write to sparseFile: %w", err)
		}
	}

	return nil
}

func (pr *Reader) Close() error {
	// No persistent resources to close
	return nil
}

// Stats returns download statistics
func (pr *Reader) Stats() map[string]interface{} {

	return map[string]interface{}{
		"bytes_downloaded": pr.bytesDownloaded.Load(),
		"active_downloads": pr.downloading.Size(),
		"chunk_size":       pr.chunkSize,
		"read_ahead":       pr.readAhead,
	}
}
