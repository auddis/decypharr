package dfs

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/logger"
	"github.com/sirrobot01/decypharr/pkg/manager"
)

// Manager manages FUSE filesystem instances with proper caching
type Manager struct {
	mounts  map[string]*Mount
	manager *manager.Manager
	logger  zerolog.Logger
	mu      sync.RWMutex
	ready   atomic.Bool
}

// NewManager creates a new  FUSE filesystem manager
func NewManager(manager *manager.Manager) *Manager {
	m := &Manager{
		manager: manager,
		logger:  logger.New("dfs"),
	}
	m.registerMounts()
	return m
}

func (m *Manager) registerMounts() {
	mounts := make(map[string]*Mount)
	for mountName := range m.manager.MountPaths() {
		mnt, err := NewMount(mountName, m.manager)
		if err != nil {
			m.logger.Error().Err(err).Msgf("Failed to create FUSE mount for debrid: %s", mountName)
			continue
		}
		mounts[mountName] = mnt
	}
	m.mu.Lock()
	m.mounts = mounts
	m.mu.Unlock()
}

// Start starts the FUSE filesystem manager
func (m *Manager) Start(ctx context.Context) error {
	var wg sync.WaitGroup
	m.mu.RLock()
	defer m.mu.RUnlock()

	for name, mount := range m.mounts {
		wg.Add(1)
		go func(name string, mount *Mount) {
			defer wg.Done()
			if err := mount.Start(ctx); err != nil {
				m.logger.Error().Err(err).Msgf("Failed to mount FUSE filesystem for debrid: %s", name)
			} else {
				m.logger.Info().Msgf("Successfully mounted FUSE filesystem for debrid: %s", name)
			}
		}(name, mount)
	}
	wg.Wait()
	m.ready.Store(true)
	return nil
}

// Stop stops the  FUSE filesystem manager
func (m *Manager) Stop() error {
	m.mu.RLock()
	defer m.mu.RUnlock()

	for name, mount := range m.mounts {
		if err := mount.Stop(); err != nil {
			m.logger.Error().Err(err).Msgf("Failed to unmount FUSE filesystem for debrid: %s", name)
		} else {
			m.logger.Info().Msgf("Successfully unmounted FUSE filesystem for debrid: %s", name)
		}
	}
	return nil
}

func (m *Manager) IsReady() bool {
	return m.ready.Load()
}

func (m *Manager) Stats() map[string]interface{} {
	m.mu.RLock()
	defer m.mu.RUnlock()

	stats := map[string]interface{}{
		"enabled": true,
		"ready":   true,
		"type":    m.Type(),
	}

	// Collect and aggregate VFS stats from all registered mounts
	if len(m.mounts) > 0 {
		mountsInfo := make(map[string]interface{})

		// Aggregated VFS stats (sum across all mounts)
		var totalCacheDirSize int64
		var totalCacheDirLimit int64
		var totalActiveReads int64
		var totalOpenedFiles int64
		var chunkSize int64
		var readAheadSize int64
		var bufferSize int64

		// Memory buffer aggregated stats
		var totalMemHits, totalMemMisses int64
		var totalEvictions, totalFlushes, totalFlushBytes int64
		var totalMemoryUsed, totalMemoryLimit int64
		var totalChunksCount, totalFilesCount int64

		mountCount := 0

		for name, mount := range m.mounts {
			mountStats := mount.Stats()
			if mountStats != nil {
				mountsInfo[name] = mountStats

				// Extract VFS stats for aggregation
				if vfsStats, ok := mountStats["stats"].(map[string]interface{}); ok {
					mountCount++

					// Sum up cache sizes
					if cacheDirSize, ok := vfsStats["cache_dir_size"].(int64); ok {
						totalCacheDirSize += cacheDirSize
					}
					if cacheDirLimit, ok := vfsStats["cache_dir_limit"].(int64); ok {
						totalCacheDirLimit += cacheDirLimit
					}

					// Sum up active operations
					if activeReads, ok := vfsStats["active_reads"].(int64); ok {
						totalActiveReads += activeReads
					}
					if openedFiles, ok := vfsStats["opened_files"].(int64); ok {
						totalOpenedFiles += openedFiles
					}

					// Aggregate memory buffer stats if present
					if memBufferStats, ok := vfsStats["memory_buffer"].(map[string]interface{}); ok {
						if hits, ok := memBufferStats["hits"].(int64); ok {
							totalMemHits += hits
						}
						if misses, ok := memBufferStats["misses"].(int64); ok {
							totalMemMisses += misses
						}
						if evictions, ok := memBufferStats["evictions"].(int64); ok {
							totalEvictions += evictions
						}
						if flushes, ok := memBufferStats["flushes"].(int64); ok {
							totalFlushes += flushes
						}
						if flushBytes, ok := memBufferStats["flush_bytes"].(int64); ok {
							totalFlushBytes += flushBytes
						}
						if memUsed, ok := memBufferStats["memory_used"].(int64); ok {
							totalMemoryUsed += memUsed
						}
						if memLimit, ok := memBufferStats["memory_limit"].(int64); ok {
							totalMemoryLimit += memLimit
						}
						if chunksCount, ok := memBufferStats["chunks_count"].(int64); ok {
							totalChunksCount += chunksCount
						}
						if filesCount, ok := memBufferStats["files_count"].(int); ok {
							totalFilesCount += int64(filesCount)
						}
					}

					// Config values (same across all mounts, just take from first)
					if mountCount == 1 {
						if cs, ok := vfsStats["chunk_size"].(int64); ok {
							chunkSize = cs
						}
						if ras, ok := vfsStats["read_ahead_size"].(int64); ok {
							readAheadSize = ras
						}
						if bs, ok := vfsStats["buffer_size"].(int64); ok {
							bufferSize = bs
						}
					}
				}
			}
		}

		stats["mounts"] = mountsInfo

		// Add aggregated VFS stats at manager level (for HTML display)
		if mountCount > 0 {
			aggregatedStats := map[string]interface{}{
				"cache_dir_size":  totalCacheDirSize,
				"cache_dir_limit": totalCacheDirLimit,
				"active_reads":    totalActiveReads,
				"opened_files":    totalOpenedFiles,
				"chunk_size":      chunkSize,
				"read_ahead_size": readAheadSize,
				"buffer_size":     bufferSize,
			}

			// Add memory buffer stats if available
			if totalFilesCount > 0 {
				hitRate := 0.0
				total := totalMemHits + totalMemMisses
				if total > 0 {
					hitRate = float64(totalMemHits) / float64(total) * 100.0
				}

				aggregatedStats["memory_buffer"] = map[string]interface{}{
					"hits":          totalMemHits,
					"misses":        totalMemMisses,
					"hit_rate_pct":  hitRate,
					"evictions":     totalEvictions,
					"flushes":       totalFlushes,
					"flush_bytes":   totalFlushBytes,
					"memory_used":   totalMemoryUsed,
					"memory_limit":  totalMemoryLimit,
					"chunks_count":  totalChunksCount,
					"files_count":   totalFilesCount,
				}
			}

			stats["stats"] = aggregatedStats
		}
	}

	return stats
}

func (m *Manager) Type() string {
	return "dfs"
}
