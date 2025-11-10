package dfs

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/logger"
	"github.com/sirrobot01/decypharr/pkg/manager"
	fuseconfig "github.com/sirrobot01/decypharr/pkg/mount/dfs/config"
	"github.com/sirrobot01/decypharr/pkg/mount/dfs/vfs"
)

// Mount implements a FUSE filesystem with sparse file caching
type Mount struct {
	fs.Inode
	vfs         *vfs.Manager
	config      *fuseconfig.FuseConfig
	logger      zerolog.Logger
	rootDir     *Dir
	unmountFunc func()
	manager     *manager.Manager
	name        string
	ready       atomic.Bool
}

// NewMount creates a new FUSE filesystem
func NewMount(mountName string, mgr *manager.Manager) (*Mount, error) {
	fuseConfig, err := fuseconfig.ParseFuseConfig(mountName)
	if err != nil {
		return nil, fmt.Errorf("failed to parse FUSE config: %w", err)
	}

	// Create read mgr
	cacheManager := vfs.NewManager(mgr, fuseConfig)

	mount := &Mount{
		vfs:     cacheManager,
		config:  fuseConfig,
		logger:  logger.New("dfs").With().Str("mount", mountName).Logger(),
		manager: mgr,
		name:    mountName,
	}
	now := time.Now()
	mount.rootDir = NewDir(cacheManager, mgr, "", LevelRoot, uint64(now.Unix()), mount.config, mount.logger)

	// Inject event into the mount
	mgr.SetEventHandlers(manager.NewEventHandlers(mount))

	return mount, nil
}

// Start starts the  FUSE filesystem
func (m *Mount) Start(ctx context.Context) error {
	m.logger.Info().
		Str("mount_path", m.config.MountPath).
		Msg("Starting DFS")

	// Create mount point if it doesn't exist(skip if on Windows)
	if runtime.GOOS != "windows" {
		if err := os.MkdirAll(m.config.MountPath, 0755); err != nil {
			return fmt.Errorf("failed to create mount point: %w", err)
		}
	}
	// Try to unmount if already mounted
	m.forceUnmount()

	mountOpt := fuse.MountOptions{
		AllowOther:           m.config.AllowOther,
		FsName:               fmt.Sprintf("dfs-%s", m.name),
		Debug:                false,
		Name:                 fmt.Sprintf("dfs-%s", m.name),
		DisableXAttrs:        true,
		IgnoreSecurityLabels: true,
	}

	var opt []string
	if m.config.AllowOther {
		opt = append(opt, "allow_other")
	}

	if runtime.GOOS == "darwin" {
		opt = append(opt, fmt.Sprintf("volname=dfs-%s", m.name))
		opt = append(opt, "noapplexattr")
		opt = append(opt, "noappledouble")
	}

	mountOpt.Options = opt

	// Configure FUSE options
	opts := &fs.Options{
		AttrTimeout:     &m.config.AttrTimeout,
		EntryTimeout:    &m.config.EntryTimeout,
		NegativeTimeout: &m.config.NegativeTimeout,
		MountOptions:    mountOpt,
		UID:             m.config.UID,
		GID:             m.config.GID,
	}

	// Start timer before creating NodeFS - adjust timeout duration as needed
	mountCtx, cancel := context.WithTimeout(ctx, m.config.DaemonTimeout)
	defer cancel()

	// Channel to receive the result of fs.Mount
	type fsResult struct {
		server *fuse.Server
		err    error
	}
	fsResultChan := make(chan fsResult, 1)

	// Run fs.Mount in a goroutine
	go func() {
		server, err := fs.Mount(m.config.MountPath, m.rootDir, opts)
		fsResultChan <- fsResult{server: server, err: err}
	}()

	var server *fuse.Server
	select {
	case result := <-fsResultChan:
		server = result.server
		if result.err != nil {
			return fmt.Errorf("failed to create mount: %w", result.err)
		}
	case <-mountCtx.Done():
		return fmt.Errorf("timeout creating mount: %w", mountCtx.Err())
	}

	// Now wait for the mount to be ready with the same timeout context
	m.logger.Info().
		Str("mount_path", m.config.MountPath).
		Msg("Waiting for DFS to be ready")

	waitChan := make(chan error, 1)
	go func() {
		waitChan <- server.WaitMount()
	}()

	select {
	case err := <-waitChan:
		if err != nil {
			_ = server.Unmount() // cleanup on error
			return fmt.Errorf("failed to wait for mount: %w", err)
		}
	case <-mountCtx.Done():
		_ = server.Unmount() // cleanup on timeout
		return fmt.Errorf("timeout waiting for mount to be ready: %w", mountCtx.Err())
	}

	umount := func() {
		m.logger.Info().Msg("Unmounting DFS")

		// Close range manager
		if m.vfs != nil {
			if err := m.vfs.Close(); err != nil {
				m.logger.Warn().Err(err).Msg("Failed to close VFS")
			}
		}

		_ = server.Unmount()
		time.Sleep(1 * time.Second)

		// Check if still mounted
		if _, err := os.Stat(m.config.MountPath); err == nil {
			m.logger.Warn().Msg("FUSE filesystem still mounted, attempting force unmount")
			m.forceUnmount()
		}
	}

	m.unmountFunc = umount
	m.ready.Store(true)
	m.logger.Info().
		Str("mount_path", m.config.MountPath).
		Msg("FUSE filesystem mounted successfully")
	return nil
}

// Stop stops the  FUSE filesystem
func (m *Mount) Stop() error {
	m.logger.Info().Msg("Stopping  FUSE filesystem")

	// Unmount first
	if m.unmountFunc != nil {
		m.unmountFunc()
	} else {
		// Use force unmount
		m.forceUnmount()
	}

	if m.vfs != nil {
		if err := m.vfs.Close(); err != nil {
			m.logger.Warn().Err(err).Msg("Failed to close vfs")
		}
	}
	return nil
}

func (m *Mount) Stats() map[string]interface{} {
	var mountStats map[string]interface{}
	if m.vfs != nil {
		vfsStats := m.vfs.GetStats()
		mountStats = map[string]interface{}{
			"name":       m.name,
			"type":       m.Type(),
			"mounted":    true,
			"mount_path": m.config.MountPath,
			"stats":      vfsStats,
		}
	}
	return mountStats
}

func (m *Mount) Type() string {
	return "dfs"
}

func (m *Mount) IsReady() bool {
	return m.ready.Load()
}

// Getattr returns root directory attributes
func (m *Mount) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Mode = 0755 | fuse.S_IFDIR
	out.Nlink = 2 // Directories have 2 links (itself + "." entry)
	out.Uid = m.config.UID
	out.Gid = m.config.GID
	now := time.Now()
	out.Atime = uint64(now.Unix())
	out.Mtime = uint64(now.Unix())
	out.Ctime = uint64(now.Unix())
	out.AttrValid = uint64(m.config.AttrTimeout.Seconds())
	return 0
}

// Lookup looks up entries in the root directory
func (m *Mount) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	return m.rootDir.Lookup(ctx, name, out)
}

// Readdir reads the root directory entries
func (m *Mount) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	return m.rootDir.Readdir(ctx)
}

// forceUnmount attempts to force unmount a path using system commands
func (m *Mount) forceUnmount() {
	methods := [][]string{
		{"umount", m.config.MountPath},
		{"umount", "-l", m.config.MountPath}, // lazy unmount
		{"fusermount", "-uz", m.config.MountPath},
		{"fusermount3", "-uz", m.config.MountPath},
	}

	for _, method := range methods {
		if err := m.tryUnmountCommand(method...); err == nil {
			return
		}
	}
}

// tryUnmountCommand tries to run an unmount command
func (m *Mount) tryUnmountCommand(args ...string) error {
	if len(args) == 0 {
		return fmt.Errorf("no command provided")
	}

	cmd := exec.Command(args[0], args[1:]...)
	return cmd.Run()
}

func (m *Mount) Refresh(dirs []string) error {
	for _, dir := range dirs {
		go m.refreshDirectory(dir)
	}

	return nil
}

// refreshDirectory navigates to a specific directory path and refreshes it
func (m *Mount) refreshDirectory(name string) {
	// Handle root directory refresh
	child, ok := m.rootDir.children.Load(name)
	if !ok {
		m.logger.Warn().Str("dir", name).Msg("Directory not found for refresh")
		return
	}
	dir, ok := child.node.(*Dir)
	if !ok {
		m.logger.Warn().Str("dir", name).Msg("MountPath is not a directory")
		return
	}
	dir.Refresh()
}
