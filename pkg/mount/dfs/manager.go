package dfs

import (
	"context"
	"sync"
)

var (
	globalManager *Manager
	globalOnce    sync.Once
)

// Manager manages FUSE filesystem instances with proper caching
type Manager struct {
	mounts map[string]*Mount
	mu     sync.RWMutex
}

// NewManager creates a new  FUSE filesystem manager
func NewManager() *Manager {
	globalOnce.Do(func() {
		globalManager = &Manager{
			mounts: make(map[string]*Mount),
		}
	})
	return globalManager
}

// GetGlobalManager returns the global DFS manager instance (used to avoid import cycles)
func GetGlobalManager() *Manager {
	return globalManager
}

// RegisterMount registers a mount instance
func (m *Manager) RegisterMount(name string, mount *Mount) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.mounts[name] = mount
}

// UnregisterMount removes a mount instance
func (m *Manager) UnregisterMount(name string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.mounts, name)
}

// Start starts the FUSE filesystem manager
func (m *Manager) Start(ctx context.Context) error {
	// This doesn't have any preparation, Mount.Start handles starting mount for each debrid
	return nil
}

// Stop stops the  FUSE filesystem manager
func (m *Manager) Stop() error {
	// Mounter handles this
	return nil
}

func (m *Manager) IsReady() bool {
	return true
}

func (m *Manager) GetStats() (map[string]interface{}, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	stats := map[string]interface{}{
		"enabled": true,
		"ready":   true,
		"type":    m.Type(),
	}

	// Collect stats from all registered mounts
	if len(m.mounts) > 0 {
		mountsInfo := make(map[string]interface{})
		var totalCacheDirSize, totalCacheDirLimit, totalActiveReads, totalOpenedFiles int64

		for name, mount := range m.mounts {
			if mount != nil && mount.vfs != nil {
				vfsStats := mount.vfs.GetStats()
				mountInfo := map[string]interface{}{
					"name":       name,
					"mounted":    true,
					"mount_path": mount.config.MountPath,
					"stats":      vfsStats,
				}
				mountsInfo[name] = mountInfo

				// Aggregate stats
				if cacheDirSize, ok := vfsStats["cache_dir_size"].(int64); ok {
					totalCacheDirSize += cacheDirSize
				}
				if cacheDirLimit, ok := vfsStats["cache_dir_limit"].(int64); ok {
					totalCacheDirLimit += cacheDirLimit
				}
				if activeReads, ok := vfsStats["active_reads"].(int64); ok {
					totalActiveReads += activeReads
				}
				if openedFiles, ok := vfsStats["opened_files"].(int64); ok {
					totalOpenedFiles += openedFiles
				}
			}
		}

		stats["mounts"] = mountsInfo

		// Add aggregated stats
		stats["stats"] = map[string]interface{}{
			"cache_dir_size":  totalCacheDirSize,
			"cache_dir_limit": totalCacheDirLimit,
			"active_reads":    totalActiveReads,
			"opened_files":    totalOpenedFiles,
		}
	}

	return stats, nil
}

// Type returns the type of mount manager
func (m *Manager) Type() string {
	return "dfs"
}
