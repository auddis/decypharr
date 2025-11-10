package vfs

import (
	"encoding/json"
	"os"
	"path/filepath"
	"time"

	"github.com/sirrobot01/decypharr/pkg/mount/dfs/vfs/ranges"
)

// Metadata stores information about a cached file
// This is persisted to disk as JSON in the vfsMeta directory
type Metadata struct {
	ModTime     time.Time      `json:"mod_time"`    // Modification time of remote file
	ATime       time.Time      `json:"atime"`       // Last access time (for LRU)
	Size        int64          `json:"size"`        // Total file size
	Ranges      []ranges.Range `json:"ranges"`      // Which parts are cached
	Fingerprint string         `json:"fingerprint"` // Hash/ETag for cache validation
	Dirty       bool           `json:"dirty"`       // Has unflushed writes
}

// scanMetadataDirectory scans the metadata directory and returns total cached size and file list
// This is much faster than scanning actual cache files
func (m *Manager) scanMetadataDirectory() (int64, []cachedFileInfo, error) {
	metaRoot := filepath.Join(m.config.CacheDir, ".meta")

	var totalSize int64
	var fileList []cachedFileInfo

	err := filepath.Walk(metaRoot, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return nil // Skip errors
		}

		// Only process .json metadata files
		if info.IsDir() || filepath.Ext(path) != ".json" {
			return nil
		}

		// Load metadata to get actual cached size and access time
		file, err := os.Open(path)
		if err != nil {
			return nil
		}
		defer file.Close()

		var meta Metadata
		if err := json.NewDecoder(file).Decode(&meta); err != nil {
			return nil // Skip corrupted metadata
		}

		// Calculate cached size from ranges
		var cachedSize int64
		for _, r := range meta.Ranges {
			cachedSize += r.Size
		}
		totalSize += cachedSize

		// Extract torrent and file name from path
		relPath, _ := filepath.Rel(metaRoot, path)
		relPath = relPath[:len(relPath)-5] // Remove .json extension

		fileList = append(fileList, cachedFileInfo{
			cacheKey:   relPath,
			accessTime: meta.ATime,
		})

		return nil
	})

	return totalSize, fileList, err
}
