package vfs

import (
	"context"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/puzpuzpuz/xsync/v4"
	"github.com/sirrobot01/decypharr/pkg/debrid/store"
	"github.com/sirrobot01/decypharr/pkg/debrid/types"
	"github.com/sirrobot01/decypharr/pkg/mount/dfs/config"
)

// StatsTracker is a lightweight struct for tracking VFS statistics
type StatsTracker struct {
	activeReads atomic.Int64
	openedFiles atomic.Int64
}

// TrackActiveRead increments/decrements active read counter
func (st *StatsTracker) TrackActiveRead(delta int64) {
	if st != nil {
		st.activeReads.Add(delta)
	}
}

func (st *StatsTracker) TrackOpenFiles(delta int64) {
	if st != nil {
		st.openedFiles.Add(delta)
	}
}

// CacheType represents the type of caching to perform
type CacheType int

const (
	CacheTypeOther CacheType = iota
	CacheTypeFFProbe
)

// String returns the string representation of the cache type
func (ct CacheType) String() string {
	switch ct {
	case CacheTypeFFProbe:
		return "ffprobe"
	case CacheTypeOther:
		return "other"
	default:
		return "unknown"
	}
}

// CacheRequest represents a request to cache a file range
type CacheRequest struct {
	TorrentName string
	FileName    string
	FileSize    int64
	StartOffset int64
	EndOffset   int64
	CacheType   CacheType
}

// FileAccessInfo tracks file access patterns for smart caching
type FileAccessInfo struct {
	TorrentName     string
	FileName        string
	LastAccessTime  time.Time
	LastReadOffset  int64
	FileSize        int64
	AccessCount     atomic.Int64
	IsNearEnd       atomic.Bool
	NextEpisode     string // Next episode filename if detected
	NextEpisodePath string // Full path to next episode
}

// Manager manages sparse files for all remote files
type Manager struct {
	debrid      *store.Cache
	config      *config.FuseConfig
	files       *lru.Cache[string, *SparseFile]
	mu          sync.RWMutex
	closeCtx    context.Context
	closeCancel context.CancelFunc

	// Stats tracker for passing to readers/files
	stats *StatsTracker

	// Smart caching: track file access for episode detection
	fileAccessTracker *xsync.Map[string, *FileAccessInfo]

	// Cached directory size (updated during cleanup)
	cachedDirSize atomic.Int64
	lastSizeCheck atomic.Int64 // Unix timestamp
}

// NewManager creates a sparseFile manager
func NewManager(debridCache *store.Cache, fuseConfig *config.FuseConfig) *Manager {
	// Each SparseFile holds: open FD + lazy-loaded ranges + state
	files, _ := lru.NewWithEvict(50, func(key string, sf *SparseFile) {
		_ = sf.Close()
	})
	ctx, cancel := context.WithCancel(context.Background())
	m := &Manager{
		config:      fuseConfig,
		debrid:      debridCache,
		files:       files,
		closeCtx:    ctx,
		closeCancel: cancel,
	}

	// Create stats tracker that references the Manager's atomic counters
	m.stats = &StatsTracker{}
	go m.closeIdleFilesLoop()
	go m.Cleanup(ctx)

	// Initialize file access tracker for smart caching
	m.fileAccessTracker = xsync.NewMap[string, *FileAccessInfo]()

	return m
}

func (m *Manager) closeIdleFilesLoop() {
	ticker := time.NewTicker(2 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			m.closeIdleFiles()
		case <-m.closeCtx.Done():
			return
		}
	}
}

func (m *Manager) closeIdleFiles() {
	threshold := time.Now().Add(-m.config.FileIdleTimeout)

	m.mu.RLock()
	keys := m.files.Keys()
	m.mu.RUnlock()

	for _, key := range keys {
		m.mu.RLock()
		sf, ok := m.files.Peek(key)
		m.mu.RUnlock()

		if ok {
			sf.mu.RLock()
			shouldClose := sf.lastAccess.Before(threshold) && sf.file != nil
			sf.mu.RUnlock()

			if shouldClose {
				_ = sf.closeFD()
			}
		}
	}
}

// GetOrCreateFile gets or creates a sparse file for caching
func (m *Manager) GetOrCreateFile(torrentName, filename string, size int64) (*SparseFile, error) {
	key := sanitizeForPath(filepath.Join(torrentName, filename))

	m.mu.RLock()
	if sf, ok := m.files.Get(key); ok {
		m.mu.RUnlock()
		// Verify the sparse file still exists on disk
		if !m.sparseFileExists(sf) {
			// File was deleted, remove from cache and recreate
			m.files.Remove(key)
		} else {
			return sf, nil
		}
	} else {
		m.mu.RUnlock()
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	// Double check after acquiring write lock
	if sf, ok := m.files.Get(key); ok {
		if m.sparseFileExists(sf) {
			return sf, nil
		}
		// File was deleted, remove from cache
		m.files.Remove(key)
	}

	sf, err := newSparseFile(m.config.CacheDir, torrentName, filename, size, m.config.ChunkSize, m.stats, m)
	if err != nil {
		return nil, err
	}

	m.files.Add(key, sf)
	m.stats.TrackOpenFiles(1) // Track open file
	return sf, nil
}

// sparseFileExists checks if the sparse file exists on disk
func (m *Manager) sparseFileExists(sf *SparseFile) bool {
	_, err := os.Stat(sf.path)
	return err == nil
}

// CreateReader creates a reader optimized for scan operations
func (m *Manager) CreateReader(torrentName string, torrentFile types.File) (*Handle, error) {
	sf, err := m.GetOrCreateFile(torrentName, torrentFile.Name, torrentFile.Size)
	if err != nil {
		return nil, err
	}

	// For scans, use smaller chunks and more aggressive concurrency
	chunkSize := m.config.ChunkSize
	readAhead := m.config.ReadAheadSize
	maxConcurrent := m.config.MaxConcurrentReads

	if readAhead == 0 {
		readAhead = chunkSize * 2 // Minimum 2 chunks ahead for smooth playback
	}

	// Prevent deadlock: semaphore size must be at least 1
	if maxConcurrent < 1 {
		maxConcurrent = 1
	}

	reader := NewReader(m.debrid, torrentName, torrentFile, sf, chunkSize, readAhead, maxConcurrent, m.stats, m)
	return NewHandle(reader), nil
}

// Close closes all sparse files
func (m *Manager) Close() error {
	m.closeCancel()

	m.files.Purge() // Calls evict callback for all entries
	return nil
}

func (m *Manager) CloseFile(filePath string) error {
	if _, exists := m.files.Peek(filePath); exists {
		m.stats.TrackOpenFiles(-1) // Decrement open file count
	}
	m.files.Remove(filePath) // Calls evict callback
	return nil
}

func (m *Manager) RemoveFile(filePath string) error {
	if sf, exists := m.files.Peek(filePath); exists {
		if err := sf.removeFromDisk(); err != nil {
			return err
		}
		m.stats.TrackOpenFiles(-1) // Decrement open file count
		m.files.Remove(filePath)
	}
	return nil
}

func (m *Manager) Cleanup(ctx context.Context) {
	// Clean up cache directory, runs every x duration
	cleanupTicker := time.NewTicker(m.config.CacheCleanupInterval)
	defer cleanupTicker.Stop()

	for {
		select {
		case <-cleanupTicker.C:
			m.cleanup()
		case <-ctx.Done():
			return
		}
	}
}

// syncAllMetadata saves metadata for all dirty files
// BUT: Only syncs files that haven't been accessed recently (not actively playing)
func (m *Manager) syncAllMetadata() {
	m.mu.RLock()
	defer m.mu.RUnlock()

	now := time.Now()

	// Iterate through all cached files
	for _, key := range m.files.Keys() {
		if sf, ok := m.files.Peek(key); ok {
			// Only sync if dirty AND not recently accessed
			// If file was accessed in last 10 seconds, skip (likely active playback)
			sf.mu.RLock()
			isDirty := sf.dirty
			recentlyAccessed := now.Sub(sf.lastAccess) < 10*time.Second
			hasRanges := sf.ranges != nil
			sf.mu.RUnlock()

			if !isDirty || !hasRanges || recentlyAccessed {
				continue // Skip files that are clean, empty, or actively playing
			}

			// Sync in background to avoid blocking
			go func(sparseFile *SparseFile) {
				sparseFile.mu.Lock()
				defer sparseFile.mu.Unlock()
				if sparseFile.dirty && sparseFile.ranges != nil {
					_ = sparseFile.saveMetadata()
					sparseFile.dirty = false
				}
			}(sf)
		}
	}
}

func (m *Manager) cleanup() {
	// Scan metadata directory first (much faster)
	// Falls back to actual cache scan if needed
	totalSize, fileList, err := m.scanMetadataDirectory()
	if err != nil || len(fileList) == 0 {
		// Fallback to actual cache scan
		totalSize, fileList, err = m.scanCacheDirectory()
		if err != nil {
			// Log error but don't crash
			return
		}
	}

	// Update cached size for stats
	m.cachedDirSize.Store(totalSize)
	m.lastSizeCheck.Store(time.Now().Unix())

	maxSize := m.config.CacheDiskSize
	if totalSize <= maxSize {
		return // Under limit, nothing to do
	}

	// Calculate how much to free - target 90% to avoid thrashing
	targetSize := maxSize * 9 / 10
	toFree := totalSize - targetSize

	// Sort by access time (least recently accessed first)
	// This gives us true LRU eviction
	sort.Slice(fileList, func(i, j int) bool {
		return fileList[i].accessTime.Before(fileList[j].accessTime)
	})

	// Remove least recently accessed files until under target
	var freed int64
	var filesRemoved int
	for _, fileInfo := range fileList {
		if freed >= toFree {
			break
		}

		// Remove the file (with proper locking and cleanup)
		if err := m.removeFile(fileInfo.cacheKey, fileInfo.path); err != nil {
			// Log but continue with other files
			continue
		}

		freed += fileInfo.size
		filesRemoved++
	}

	// Update cached size after cleanup
	if filesRemoved > 0 {
		newSize := totalSize - freed
		m.cachedDirSize.Store(newSize)
	}
}

type cachedFileInfo struct {
	path       string    // Full path to sparse file
	cacheKey   string    // Key in LRU cache (for removal)
	size       int64     // Actual disk usage (sparse-aware)
	accessTime time.Time // Last access time (for LRU eviction)
}

// scanCacheDirectory walks the cache directory and returns total size and file list
func (m *Manager) scanCacheDirectory() (int64, []cachedFileInfo, error) {
	var totalSize int64
	var fileList []cachedFileInfo
	seenFiles := make(map[string]bool)

	err := filepath.Walk(m.config.CacheDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if info.IsDir() {
			return nil
		}

		// Skip metadata directory
		if strings.Contains(path, ".meta") {
			return nil
		}

		// Skip if already processed
		if seenFiles[path] {
			return nil
		}
		seenFiles[path] = true

		// Get actual disk usage (accounts for sparse files)
		fileSize, err := m.getActualDiskUsage(path, info)
		if err != nil {
			// Fallback to logical size
			fileSize = info.Size()
		}

		// Calculate cache key from path
		cacheKey, err := filepath.Rel(m.config.CacheDir, path)
		if err != nil {
			cacheKey = "" // Will skip removal if can't calculate key
		}

		// Get access time - prefer from in-memory tracking
		accessTime := m.getFileAccessTime(cacheKey, info)

		totalSize += fileSize
		fileList = append(fileList, cachedFileInfo{
			path:       path,
			cacheKey:   cacheKey,
			size:       fileSize,
			accessTime: accessTime,
		})

		return nil
	})

	return totalSize, fileList, err
}

// getFileAccessTime is implemented in platform-specific files:
// - cache_unix.go (Linux, FreeBSD, OpenBSD, NetBSD)
// - cache_darwin.go (macOS)
// - cache_windows.go (Windows)

// getActualDiskUsage returns the actual disk space used by a file (accounting for sparse files)
func (m *Manager) getActualDiskUsage(path string, info os.FileInfo) (int64, error) {
	// Get the underlying syscall.Stat_t structure
	sys := info.Sys()
	if sys == nil {
		return info.Size(), nil
	}

	// Platform-specific handling
	switch stat := sys.(type) {
	case *syscall.Stat_t:
		// Linux/Unix: blocks are 512 bytes, Blocks field gives count
		// Actual size = Blocks * 512
		return stat.Blocks * 512, nil
	default:
		// Fallback for unsupported platforms
		return info.Size(), nil
	}
}

// removeFile removes a file from both in-memory cache and disk (thread-safe)
func (m *Manager) removeFile(cacheKey, diskPath string) error {
	// First, remove from in-memory LRU cache (with locking)
	if cacheKey != "" {
		m.mu.Lock()
		if sf, exists := m.files.Peek(cacheKey); exists {
			// Close the file cleanly before removing
			_ = sf.Close()
			m.files.Remove(cacheKey)
			m.stats.TrackOpenFiles(-1)
		}
		m.mu.Unlock()
	}

	// Then remove from disk
	return m.removeFileFromDisk(diskPath)
}

// removeFileFromDisk removes a sparse file from disk
func (m *Manager) removeFileFromDisk(sparseFilePath string) error {
	// Remove sparse file
	if err := os.Remove(sparseFilePath); err != nil && !os.IsNotExist(err) {
		return err
	}

	return nil
}

// GetStats returns VFS cache statistics
func (m *Manager) GetStats() map[string]interface{} {
	// Use cached directory size if recent (within 30 seconds)
	now := time.Now().Unix()
	lastCheck := m.lastSizeCheck.Load()
	cachedSize := m.cachedDirSize.Load()

	var totalSize int64
	if cachedSize > 0 && (now-lastCheck) < 30 {
		// Use cached value (fast)
		totalSize = cachedSize
	} else {
		// Scan metadata directory (MUCH faster than scanning actual files)
		// Falls back to actual cache scan if metadata doesn't exist
		size, _, err := m.scanMetadataDirectory()
		if err != nil || size == 0 {
			// Fallback to actual cache scan
			size, _, _ = m.scanCacheDirectory()
		}
		totalSize = size
		m.cachedDirSize.Store(size)
		m.lastSizeCheck.Store(now)
	}

	stats := map[string]interface{}{
		"cache_dir_size":  totalSize,
		"cache_dir_limit": m.config.CacheDiskSize,
		"active_reads":    m.stats.activeReads.Load(),
		"opened_files":    m.stats.openedFiles.Load(),
		"chunk_size":      m.config.ChunkSize,
		"read_ahead_size": m.config.ReadAheadSize,
		"buffer_size":     m.config.BufferSize,
	}

	return stats
}

// prefetchNextEpisode prefetches the beginning of the next episode
func (m *Manager) prefetchNextEpisode(ctx context.Context, torrentName string, nextEpisode types.File) {
	// Get or create sparse file for next episode
	sf, err := m.GetOrCreateFile(torrentName, nextEpisode.Name, nextEpisode.Size)
	if err != nil {
		return
	}

	// Create a reader for the next episode
	reader := NewReader(m.debrid, torrentName, nextEpisode, sf, m.config.ChunkSize, m.config.ReadAheadSize, m.config.MaxConcurrentReads, m.stats, m)

	// Prefetch first few chunks (enough for instant start of next episode)
	numChunksToPrefetch := int64(3) // Prefetch first 3 chunks (24MB with 8MB chunks)
	totalChunks := (nextEpisode.Size + m.config.ChunkSize - 1) / m.config.ChunkSize
	if numChunksToPrefetch > totalChunks {
		numChunksToPrefetch = totalChunks
	}

	// Download chunks in background
	for chunkIdx := int64(0); chunkIdx < numChunksToPrefetch; chunkIdx++ {
		go reader.downloadChunkAsync(ctx, chunkIdx)
	}
}

// === Utility Functions ===

// sanitizeForPath makes a string safe for use in file paths
func sanitizeForPath(name string) string {
	// Replace problematic characters with underscores
	replacer := strings.NewReplacer(
		"/", "_",
		"\\", "_",
		":", "_",
		"*", "_",
		"?", "_",
		"\"", "_",
		"<", "_",
		">", "_",
		"|", "_",
	)
	sanitized := replacer.Replace(name)

	// Limit length to prevent filesystem issues
	if len(sanitized) > 200 {
		sanitized = sanitized[:200]
	}

	return sanitized
}
