package vfs

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/sirrobot01/decypharr/pkg/mount/dfs/vfs/ranges"
)

// SparseFile represents a cached file on disk with range-based tracking
// Uses range map for accurate, efficient sparse file handling (platform-independent)
// Ranges are lazy-loaded from metadata to save memory
type SparseFile struct {
	path         string
	torrentName  string
	fileName     string
	size         int64
	chunkSize    int64
	file         *os.File
	ranges       *ranges.Ranges // Lazy-loaded from metadata
	rangesLoaded bool           // Whether ranges have been loaded
	mu           sync.RWMutex
	lastAccess   time.Time
	modTime      time.Time
	stats        *StatsTracker
	cacheManager *Manager // Reference to save metadata
	dirty        bool     // Has unflushed changes to metadata
}

// newSparseFile creates or opens a sparse cached file
func newSparseFile(cacheDir, torrentName, fileName string, size, chunkSize int64, stats *StatsTracker, manager *Manager) (*SparseFile, error) {
	sanitizedFileName := sanitizeForPath(fileName)
	torrentDir := filepath.Join(cacheDir, sanitizeForPath(torrentName))
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
	if err := file.Truncate(size); err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("truncate cache file: %w", err)
	}

	sf := &SparseFile{
		path:         cachePath,
		torrentName:  torrentName,
		fileName:     fileName,
		size:         size,
		chunkSize:    chunkSize,
		file:         file,
		ranges:       nil, // Lazy-loaded
		rangesLoaded: false,
		stats:        stats,
		lastAccess:   time.Now(),
		modTime:      time.Now(),
		cacheManager: manager,
		dirty:        false,
	}

	return sf, nil
}

// loadRanges lazy-loads ranges by scanning the actual file
// Ranges are always rebuilt from actual data (fast enough with sampling)
func (sf *SparseFile) loadRanges() error {
	if sf.rangesLoaded {
		return nil // Already loaded
	}

	// Scan existing file to rebuild ranges
	sf.ranges = ranges.New()
	if err := sf.scanExistingData(); err != nil {
		return err
	}

	sf.rangesLoaded = true
	return nil
}

// scanExistingData scans the file to detect which chunks already have data
// This is a fallback when metadata doesn't exist
func (sf *SparseFile) scanExistingData() error {
	// Read file in chunks and check if they contain non-zero data
	// This is a heuristic - we check a few bytes per chunk for performance

	numChunks := (sf.size + sf.chunkSize - 1) / sf.chunkSize
	sample := make([]byte, 4096) // Sample first 4KB of each chunk

	for chunkIdx := int64(0); chunkIdx < numChunks; chunkIdx++ {
		offset := chunkIdx * sf.chunkSize
		readSize := int64(len(sample))
		if offset+readSize > sf.size {
			readSize = sf.size - offset
		}

		n, err := sf.file.ReadAt(sample[:readSize], offset)
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
			chunkEnd := offset + sf.chunkSize
			if chunkEnd > sf.size {
				chunkEnd = sf.size
			}
			sf.ranges.Insert(ranges.Range{
				Pos:  offset,
				Size: chunkEnd - offset,
			})
		}
	}

	return nil
}

// saveMetadata persists the current state to disk
func (sf *SparseFile) saveMetadata() error {
	if sf.cacheManager == nil || sf.ranges == nil {
		return nil
	}

	meta := &Metadata{
		ModTime:     sf.modTime,
		ATime:       sf.lastAccess,
		Size:        sf.size,
		Ranges:      sf.ranges.GetRanges(),
		Fingerprint: "", // Could add ETag/hash here
		Dirty:       sf.dirty,
	}

	if err := sf.cacheManager.saveMetadata(sf.torrentName, sf.fileName, meta); err != nil {
		return fmt.Errorf("save metadata: %w", err)
	}

	return nil
}

// ReadAt reads from cache if data is available, returns false if not cached
func (sf *SparseFile) ReadAt(p []byte, offset int64) (n int, cached bool, err error) {
	// Ensure ranges are loaded (may need to upgrade to write lock)
	sf.mu.RLock()
	if !sf.rangesLoaded {
		sf.mu.RUnlock()
		sf.mu.Lock()
		if err := sf.loadRanges(); err != nil {
			sf.mu.Unlock()
			return 0, false, err
		}
		sf.mu.Unlock()
		sf.mu.RLock()
	}
	defer sf.mu.RUnlock()

	sf.lastAccess = time.Now()

	// Check if requested range is fully cached
	requestedRange := ranges.Range{Pos: offset, Size: int64(len(p))}
	if !sf.ranges.Present(requestedRange) {
		return 0, false, nil // Not cached
	}

	// Data is cached, read from disk
	n, err = sf.file.ReadAt(p, offset)
	if err != nil {
		return n, false, err
	}

	return n, true, nil
}

// WriteAt writes data and updates the range map
func (sf *SparseFile) WriteAt(p []byte, offset int64) (int, error) {
	sf.mu.Lock()
	defer sf.mu.Unlock()

	// Ensure ranges are loaded
	if !sf.rangesLoaded {
		if err := sf.loadRanges(); err != nil {
			return 0, err
		}
	}

	// Write to file
	n, err := sf.file.WriteAt(p, offset)
	if err != nil {
		return n, err
	}

	// Mark this range as cached
	if n > 0 {
		sf.ranges.Insert(ranges.Range{
			Pos:  offset,
			Size: int64(n),
		})
		sf.dirty = true
	}

	sf.lastAccess = time.Now()
	return n, nil
}

// Sync flushes data to disk
func (sf *SparseFile) Sync() error {
	sf.mu.Lock()
	defer sf.mu.Unlock()

	// Sync file data
	if sf.file != nil {
		return sf.file.Sync()
	}

	return nil
}

// closeFD closes the file descriptor
func (sf *SparseFile) closeFD() error {
	sf.mu.Lock()
	defer sf.mu.Unlock()

	if sf.file != nil {
		err := sf.file.Close()
		sf.file = nil
		return err
	}
	return nil
}

// Close closes the sparse file
func (sf *SparseFile) Close() error {
	sf.mu.Lock()
	defer sf.mu.Unlock()

	if sf.file != nil {
		err := sf.file.Close()
		sf.file = nil
		return err
	}
	return nil
}

// removeFromDisk removes the sparse file from disk
func (sf *SparseFile) removeFromDisk() error {
	sf.mu.Lock()
	defer sf.mu.Unlock()

	// Close file if open
	if sf.file != nil {
		_ = sf.file.Close()
		sf.file = nil
	}

	// Remove sparse file
	if err := os.Remove(sf.path); err != nil && !os.IsNotExist(err) {
		return err
	}

	return nil
}

// GetCachedSize returns the total bytes downloaded
func (sf *SparseFile) GetCachedSize() int64 {
	sf.mu.RLock()
	if !sf.rangesLoaded {
		sf.mu.RUnlock()
		sf.mu.Lock()
		_ = sf.loadRanges()
		sf.mu.Unlock()
		sf.mu.RLock()
	}
	defer sf.mu.RUnlock()

	if sf.ranges == nil {
		return 0
	}
	return sf.ranges.Size()
}

// GetCachedRanges returns all cached ranges (for debugging/stats)
func (sf *SparseFile) GetCachedRanges() []ranges.Range {
	sf.mu.RLock()
	if !sf.rangesLoaded {
		sf.mu.RUnlock()
		sf.mu.Lock()
		_ = sf.loadRanges()
		sf.mu.Unlock()
		sf.mu.RLock()
	}
	defer sf.mu.RUnlock()

	if sf.ranges == nil {
		return nil
	}
	return sf.ranges.GetRanges()
}

// FindMissing returns ranges that need to be downloaded
func (sf *SparseFile) FindMissing(offset, length int64) []ranges.Range {
	sf.mu.RLock()
	if !sf.rangesLoaded {
		sf.mu.RUnlock()
		sf.mu.Lock()
		_ = sf.loadRanges()
		sf.mu.Unlock()
		sf.mu.RLock()
	}
	defer sf.mu.RUnlock()

	if sf.ranges == nil {
		// If ranges failed to load, assume everything is missing
		return []ranges.Range{{Pos: offset, Size: length}}
	}

	return sf.ranges.FindMissing(ranges.Range{
		Pos:  offset,
		Size: length,
	})
}

// IsCached checks if a range is cached WITHOUT allocating buffers
// This is much more efficient than ReadAt for cache checks
func (sf *SparseFile) IsCached(offset, length int64) bool {
	sf.mu.RLock()
	if !sf.rangesLoaded {
		sf.mu.RUnlock()
		sf.mu.Lock()
		if !sf.rangesLoaded { // Double-check after acquiring write lock
			_ = sf.loadRanges()
		}
		sf.mu.Unlock()
		sf.mu.RLock()
	}
	defer sf.mu.RUnlock()

	if sf.ranges == nil {
		return false
	}

	return sf.ranges.Present(ranges.Range{
		Pos:  offset,
		Size: length,
	})
}
