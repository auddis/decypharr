package vfs

import (
	"path/filepath"
)

// BuildCacheKey returns the canonical cache key used for tracking files in the VFS manager.
// The key mirrors the on-disk layout (sanitized parent directory + sanitized filename).
func BuildCacheKey(parent, name string) string {
	sanitizedParent := sanitizeForPath(parent)
	sanitizedName := sanitizeForPath(name)

	if sanitizedParent == "" {
		return sanitizedName
	}

	return filepath.Join(sanitizedParent, sanitizedName)
}
