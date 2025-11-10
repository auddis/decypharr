package manager

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/utils"
	"github.com/sirrobot01/decypharr/pkg/storage"
	"github.com/sirrobot01/decypharr/pkg/version"
)

const (
	EntryAllFolder     string = "__all__"
	EntryBadFolder     string = "__bad__"
	EntryTorrentFolder string = "torrents"
)

// FileInfo implements os.FileInfo
type FileInfo struct {
	name         string
	size         int64
	mode         os.FileMode
	modTime      time.Time
	isDir        bool
	content      []byte
	parent       string
	activeDebrid string
	canDelete    bool
	byteRange    *[2]int64
}

func (f *FileInfo) Name() string         { return f.name }
func (f *FileInfo) Size() int64          { return f.size }
func (f *FileInfo) Mode() os.FileMode    { return f.mode }
func (f *FileInfo) ModTime() time.Time   { return f.modTime }
func (f *FileInfo) IsDir() bool          { return f.isDir }
func (f *FileInfo) Sys() interface{}     { return nil }
func (f *FileInfo) Content() []byte      { return f.content }
func (f *FileInfo) Parent() string       { return f.parent }
func (f *FileInfo) ActiveDebrid() string { return f.activeDebrid }
func (f *FileInfo) CanDelete() bool      { return f.canDelete }
func (f *FileInfo) IsRemote() bool       { return len(f.content) == 0 }
func (f *FileInfo) ByteRange() *[2]int64 { return f.byteRange }

// GetTorrentMountPath returns the full mount path for a torrent
// Returns the path based on the new unified mount structure
func (m *Manager) GetTorrentMountPath(torrent *storage.Torrent) string {
	return filepath.Join(m.config.Mount.MountPath, torrent.ActiveDebrid, EntryAllFolder, torrent.GetFolder())
}

func (m *Manager) setMountPaths() {
	cfg := config.Get()
	mountPaths := make(map[string]*FileInfo)
	for _, dc := range cfg.Debrids {
		mountPaths[dc.Name] = &FileInfo{
			name:    dc.Name,
			size:    0,
			modTime: time.Now(),
			isDir:   true,
		}
	}

	m.rootInfo = &FileInfo{
		name:    "",
		size:    0,
		modTime: time.Now(),
		isDir:   true,
	}

	m.mountPaths = mountPaths
}

func (m *Manager) MountPaths() map[string]*FileInfo {
	return m.mountPaths
}

func (m *Manager) RootInfo() *FileInfo {
	if m.rootInfo == nil {
		m.rootInfo = &FileInfo{
			name:    "",
			size:    0,
			modTime: time.Now(),
			isDir:   true,
		}
	}
	return m.rootInfo
}

// RootEntryChildren returns a list of parent directories used in the mount structure
// These are like /mnt/remote/realdebrid
func (m *Manager) RootEntryChildren() []FileInfo {
	infos := make([]FileInfo, 0, len(m.mountPaths))
	for _, mount := range m.mountPaths {
		infos = append(infos, *mount)
	}
	return infos
}

func (m *Manager) GetMountInfo(name string) *FileInfo {
	mount, ok := m.mountPaths[name]
	if !ok {
		return nil
	}
	return mount
}

// GetEntries returns the subdirectories under a given mount name
// For the mount named "realdebrid", it would show __all__, __bad__, and any custom folders
// For the new "manager" mount, it would show "torrents"
func (m *Manager) GetEntries() []FileInfo {
	var subDirs []FileInfo
	extras := []string{EntryAllFolder, EntryBadFolder, EntryTorrentFolder}
	for _, dir := range extras {
		subDirs = append(subDirs, FileInfo{
			name:    dir,
			isDir:   true,
			modTime: time.Now(),
			size:    0,
		})
	}
	// AddOrUpdate custom folders
	if m.customFolders != nil {
		for _, folderName := range m.customFolders.folders {
			subDirs = append(subDirs, FileInfo{
				name:    folderName,
				isDir:   true,
				modTime: time.Now(),
				size:    0,
			})
		}
	}

	// AddOrUpdate version.txt
	subDirs = append(subDirs, FileInfo{
		name:    "version.txt",
		isDir:   false,
		modTime: time.Now(),
		size:    int64(len(version.GetInfo().String())),
		content: []byte(version.GetInfo().Version),
	})
	return subDirs
}

func (m *Manager) GetEntryChildren(group string) (*FileInfo, []FileInfo) {
	return m.entry.Get(group)
}

func (m *Manager) GetTorrentChildren(name string) (*FileInfo, []FileInfo) {
	return m.entry.Get("torrent::" + name)
}

func (m *Manager) GetTorrentEntry(torrentName string) (*FileInfo, error) {
	current, _ := m.GetTorrentChildren(torrentName)
	if current == nil {
		return nil, fmt.Errorf("torrent %s not found", torrentName)
	}
	return current, nil
}

func (m *Manager) GetTorrentFile(torrentName, fileName string) (*FileInfo, error) {
	torr, err := m.GetTorrentByName(torrentName)
	if err != nil {
		return nil, fmt.Errorf("torrent %s not found", torrentName)
	}
	file, err := torr.GetFile(fileName)
	if err != nil {
		return nil, fmt.Errorf("file %s not found in torrent %s", fileName, torrentName)
	}
	return &FileInfo{
		name:      file.Name,
		size:      file.Size,
		modTime:   torr.AddedOn,
		isDir:     false,
		parent:    torr.Folder,
		canDelete: true,
		byteRange: file.ByteRange,
	}, nil
}

// getEntryChildren
// Groups are __all__, __bad__, custom folders, and paginated
func (m *Manager) getEntryChildren(group string) (*FileInfo, []FileInfo) {
	currentDir := &FileInfo{
		name:    group,
		size:    0,
		modTime: time.Now(),
		isDir:   true,
	}
	switch group {
	case EntryAllFolder, EntryTorrentFolder:
		// This returns all torrents
		torrents, err := m.GetTorrents(nil)
		if err != nil {
			return nil, nil
		}
		return currentDir, convertTorrentsToFileInfo(torrents)
	case EntryBadFolder:
		torrents, err := m.GetTorrents(func(t *storage.Torrent) bool {
			return t.Bad
		})
		if err != nil {
			return nil, nil
		}
		return currentDir, convertTorrentsToFileInfo(torrents)
	case "version.txt":
		currentDir.content = []byte(version.GetInfo().Version)
		currentDir.size = int64(len(currentDir.content))
		currentDir.isDir = false
		return currentDir, nil
	default:
		// Custom folder
		return currentDir, m.getCustomFolderChildren(group)
	}
}

func (m *Manager) getTorrentChildren(name string) (*FileInfo, []FileInfo) {
	// Find the torrent by folder name
	torr, err := m.GetTorrentByName(name)
	if err != nil || torr == nil {
		return nil, nil
	}

	// Convert files to FileInfo
	infos := make([]FileInfo, 0, len(torr.Files))
	for _, file := range torr.Files {
		infos = append(infos, FileInfo{
			name:      file.Name,
			size:      file.Size,
			modTime:   torr.AddedOn,
			isDir:     false,
			parent:    torr.Folder,
			canDelete: true,
			byteRange: file.ByteRange,
		})
	}

	currentDir := &FileInfo{
		name:    torr.Folder,
		size:    torr.Size,
		modTime: torr.AddedOn,
		isDir:   true,
	}
	return currentDir, infos
}

func (m *Manager) RemoveEntry(entry *FileInfo) error {
	if entry == nil {
		return fmt.Errorf("entry is nil")
	}
	if !entry.CanDelete() {
		return fmt.Errorf("entry %s cannot be deleted", entry.name)
	}

	if entry.isDir {
		// This is a torrent folder
		m.logger.Debug().Str("torrent", entry.name).Msg("Removing torrent folder")
		torr, err := m.GetTorrentByName(entry.name)
		if err != nil {
			return fmt.Errorf("torrent %s not found", entry.name)
		}
		return m.DeleteTorrent(torr.InfoHash, true)
	}
	// This is a file within a torrent
	return m.RemoveTorrentFile(entry.Parent(), entry.Name())
}

func (m *Manager) CopyEntry(entry *FileInfo, destPath string, delete bool) error {
	if entry == nil {
		return fmt.Errorf("entry is nil")
	}
	if !entry.CanDelete() {
		return fmt.Errorf("entry %s cannot be copied", entry.name)
	}
	if entry.isDir {
		// This is a torrent folder
		m.logger.Debug().Str("torrent", entry.name).Msg("Copying torrent folder")
		torr, err := m.GetTorrentByName(entry.name)
		if err != nil {
			return fmt.Errorf("torrent %s not found", entry.name)
		}
		// Create a copy of the torrent, with the new destination
		// To do this, we need to create a new torrent with the same files, but with the new folder name
		newTorrent := *torr
		newTorrent.Folder = filepath.Base(destPath)
		// Set a new infohash to avoid conflicts
		newTorrent.InfoHash = utils.GenerateInfoHash()
		err = m.AddOrUpdate(&newTorrent, func(t *storage.Torrent) {
			m.RefreshEntries(true)
		})
		if delete {
			// Delete the original torrent
			err = m.DeleteTorrent(torr.InfoHash, false) // do not delete from debrid
		}
		return err
	}
	// This is a file within a torrent

	torr, err := m.GetTorrentByName(entry.Parent())
	if err != nil {
		return fmt.Errorf("torrent %s not found", entry.Parent())
	}
	file, err := torr.GetFile(entry.Name())
	if err != nil {
		return fmt.Errorf("file %s not found in torrent %s", entry.Name(), entry.Parent())
	}
	// Create a copy of the file, with the new name
	newFile := *file
	newFile.Name = filepath.Base(destPath)
	// Add the new file to the torrent
	torr.Files[newFile.Name] = &newFile
	err = m.AddOrUpdate(torr, func(t *storage.Torrent) {
		m.RefreshEntries(true)
	})
	if delete {
		// Remove the original file
		err = m.RemoveTorrentFile(torr.Folder, file.Name)
	}
	return err

}

func (m *Manager) RemoveTorrentFile(torrentName, filename string) error {
	torr, err := m.GetTorrentByName(torrentName)
	if err != nil {
		return fmt.Errorf("torrent %s not found", torrentName)
	}
	file, err := torr.GetFile(filename)
	if err != nil {
		return fmt.Errorf("file %s not found in torrent %s", filename, torrentName)
	}
	file.Deleted = true
	torr.Files[filename] = file

	// If the torrent has no files left, delete it
	if len(torr.GetActiveFiles()) == 0 {
		m.logger.Debug().Msgf("Torrent %s has no files left, deleting it", torr.InfoHash)
		if err := m.DeleteTorrent(torr.InfoHash, true); err != nil {
			return fmt.Errorf("failed to delete torrent %s: %w", torr.InfoHash, err)
		}
		return nil
	}
	return m.AddOrUpdate(torr, func(t *storage.Torrent) {
		m.RefreshEntries(true)
	})
}

func convertTorrentsToFileInfo(torrents []*storage.Torrent) []FileInfo {
	infos := make([]FileInfo, 0, len(torrents))
	for _, t := range torrents {
		info := FileInfo{
			name:         t.Folder,
			size:         t.Size,
			modTime:      t.AddedOn,
			isDir:        true,
			activeDebrid: t.ActiveDebrid,
			canDelete:    true,
		}
		infos = append(infos, info)
	}
	return infos
}

func (m *Manager) getCustomFolderChildren(folder string) []FileInfo {
	filters := m.customFolders.filters[folder]
	if len(filters) == 0 {
		return nil
	}

	torrents, err := m.GetTorrents(func(t *storage.Torrent) bool {
		if t.Bad {
			return false
		}
		return m.customFolders.matchesFilter(folder, &FileInfo{
			name: t.Folder,
			size: t.Size,
		}, t.AddedOn)
	})
	if err != nil {
		return nil
	}
	return convertTorrentsToFileInfo(torrents)
}
