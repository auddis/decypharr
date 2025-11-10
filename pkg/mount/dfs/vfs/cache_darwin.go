//go:build darwin

package vfs

import (
	"os"
	"syscall"
	"time"
)

// getFileAccessTime returns the access time for a file (platform-specific)
func (m *Manager) getFileAccessTime(cacheKey string, fileInfo os.FileInfo) time.Time {
	// Try to get from in-memory tracking first
	if cacheKey != "" {
		if f, ok := m.files.Load(cacheKey); ok {
			if ts := f.lastAccessTime(); !ts.IsZero() {
				return ts
			}
		}
	}

	// Fallback: use file system access time (macOS)
	if stat, ok := fileInfo.Sys().(*syscall.Stat_t); ok {
		return time.Unix(stat.Atimespec.Sec, stat.Atimespec.Nsec)
	}

	// Last resort: use modification time
	return fileInfo.ModTime()
}
