# DFS (Decypharr File System) Mounting

This guide explains how to use Decypharr's native DFS mounting system, an alternative to rclone that provides optimized performance and advanced caching features.

## Overview

DFS is Decypharr's built-in FUSE filesystem that mounts your debrid services directly without requiring external rclone. It provides:

- **Native Integration**: Built directly into Decypharr, no external dependencies
- **Optimized Performance**: Designed specifically for streaming from debrid services
- **Smart Caching**: Intelligent prefetching and caching for seamless playback
- **Lower Resource Usage**: More efficient than rclone for most use cases
- **Episode-Aware**: Automatically prefetches next episodes for binge-watching

## Prerequisites

- **Docker users**: FUSE support required (similar to rclone mounting)
- **macOS users**: [macFUSE](https://osxfuse.github.io/) must be installed
- **Linux users**: FUSE typically available by default
- **Windows users**: Limited support (WebDAV recommended)

## Configuration

### Basic Setup

Add DFS configuration to your `config.json`:

```json
{
  "dfs": {
    "enabled": true,
    "mount_path": "/mnt/decypharr",
    "cache_dir": "/config/dfs/cache",
    "disk_cache_size": "10GB",
    "cache_cleanup_interval": "5m",
    "cache_expiry": "24h",

    "chunk_size": "8MB",
    "read_ahead_size": "16MB",
    "max_concurrent_reads": 4,
    "buffer_size": "4MB",

    "uid": 1000,
    "gid": 1000,
    "umask": "0022",
    "allow_other": true,
    "async_read": true,
    "default_permissions": true,

    "attr_timeout": "30s",
    "entry_timeout": "30s",
    "negative_timeout": "5s",

    "stats_interval": "1m"
  }
}
```

### Configuration Options

#### Core Settings

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `enabled` | boolean | `false` | Enable DFS mounting |
| `mount_path` | string | `/mnt/decypharr` | Base mount directory (providers mounted as subdirs) |
| `cache_dir` | string | `/config/dfs/cache` | Cache storage location |

#### Cache Settings

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `disk_cache_size` | string | `5GB` | Maximum cache size on disk |
| `cache_cleanup_interval` | string | `5m` | How often to check cache size |
| `cache_expiry` | string | `24h` | How long cached files remain valid |
| `chunk_size` | string | `8MB` | Size of each cached chunk |
| `read_ahead_size` | string | `16MB` | Amount of data to prefetch |
| `max_concurrent_reads` | int | `4` | Parallel download limit |
| `buffer_size` | string | `4MB` | In-memory buffer size for ultra-fast access |

#### Performance Settings

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `async_read` | boolean | `true` | Enable asynchronous read operations |
| `attr_timeout` | string | `30s` | File attribute cache duration |
| `entry_timeout` | string | `30s` | Directory entry cache duration |
| `daemon_timeout` | string | `10s` | FUSE daemon timeout |

#### File System Settings

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `uid` | int | `1000` | User ID for mounted files |
| `gid` | int | `1000` | Group ID for mounted files |
| `umask` | string | `"0022"` | File permission mask |
| `allow_other` | boolean | `true` | Allow other users to access mount |
| `allow_root` | boolean | `false` | Allow root to access mount |
| `default_permissions` | boolean | `true` | Enable kernel permission checking |

#### Smart Caching

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `smart_caching` | boolean | `false` | Enable intelligent episode prefetching for seamless binge-watching |

When enabled, DFS will:
- Detect when you're near the end of an episode (last 10%)
- Automatically identify the next episode in the torrent
- Prefetch the beginning of the next episode in the background
- Provide instant playback start for the next episode

**Supported Episode Patterns:**
- S01E02, s01e02 (Season/Episode format)
- 1x02 (Alternative season format)
- E02, Episode 02 (Episode-only format)

**Smart Behavior:**
- Automatically detects ffprobe/metadata scans and disables aggressive prefetching to maintain scan speed
- Only prefetches during actual playback (sequential reads)
- Uses in-memory buffer for ultra-fast access to file beginnings

## Mount Structure

When enabled, DFS mounts each debrid provider as a subdirectory:

```
/mnt/decypharr/
├── realdebrid/
│   ├── __all__/              # All torrents
│   ├── __bad__/              # Failed torrents
│   └── My.Movie.2024/        # Individual torrents
│       └── My.Movie.2024.mkv
├── torbox/
│   ├── __all__/
│   └── ...
└── alldebrid/
    ├── __all__/
    └── ...
```

### Special Directories

- `__all__/`: Contains all active torrents
- `__bad__/`: Contains torrents with errors
- Individual torrent folders: Named after the torrent

## Docker Setup

### Docker Compose

```yaml
version: '3.8'
services:
  decypharr:
    image: sirrobot01/decypharr:latest
    container_name: decypharr
    ports:
      - "8282:8282"
    volumes:
      - ./config:/config
      - /mnt:/mnt:rshared  # Important: use 'rshared' for mount propagation
    devices:
      - /dev/fuse:/dev/fuse:rwm
    cap_add:
      - SYS_ADMIN
    security_opt:
      - apparmor:unconfined
    environment:
      - PUID=1000  # Your user ID
      - PGID=1000  # Your group ID
    restart: unless-stopped
```

### Docker Run

```bash
docker run -d \
  --name decypharr \
  -p 8282:8282 \
  -v ./config:/config \
  -v /mnt:/mnt:rshared \
  --device /dev/fuse:/dev/fuse:rwm \
  --cap-add SYS_ADMIN \
  --security-opt apparmor:unconfined \
  -e PUID=1000 \
  -e PGID=1000 \
  sirrobot01/decypharr:latest
```

**Important Docker Notes:**
- Use `:rshared` mount propagation for the mount volume
- Include `/dev/fuse` device for FUSE support
- Add `SYS_ADMIN` capability for mounting

## Performance Tuning

### For Streaming

Optimize for media streaming with fast playback start:

```json
{
  "dfs": {
    "smart_caching": false,
    "chunk_size": "8MB",
    "read_ahead_size": "32MB",
    "max_concurrent_reads": 4,
    "buffer_size": "8MB",
    "disk_cache_size": "20GB"
  }
}
```

**Note:** Larger `buffer_size` provides faster playback start but uses more RAM per open file.

### For Sequential Access (Episodes)

Optimize for binge-watching with instant episode switching:

```json
{
  "dfs": {
    "smart_caching": true,
    "chunk_size": "8MB",
    "read_ahead_size": "16MB",
    "buffer_size": "4MB",
    "max_concurrent_reads": 4,
    "disk_cache_size": "15GB",
    "cache_expiry": "48h"
  }
}
```

**Smart Features:**
- Next episode prefetches automatically when you reach 90% of current episode
- Memory buffer provides instant playback start
- FFProbe scans remain fast (aggressive prefetch disabled during metadata scans)

### For Low Bandwidth

Reduce cache and prefetching:

```json
{
  "dfs": {
    "chunk_size": "4MB",
    "read_ahead_size": "8MB",
    "max_concurrent_reads": 2,
    "disk_cache_size": "5GB"
  }
}
```

### For Low Storage

Minimize disk usage:

```json
{
  "dfs": {
    "disk_cache_size": "2GB",
    "cache_cleanup_interval": "2m",
    "cache_expiry": "12h"
  }
}
```

## Monitoring

### Stats Page

View DFS statistics at: `http://localhost:8282/stats`

The stats page shows:
- Cache hit rate
- Network requests and bytes
- Read operations
- Disk usage
- Smart caching metrics (if enabled)

### Cache Management

DFS automatically manages cache based on:
- Disk space limits (`disk_cache_size`)
- File age (`cache_expiry`)
- Access patterns (with smart caching)

Manual cache clearing:
```bash
# Remove cache directory
rm -rf /config/dfs/cache/*
```

## Troubleshooting

### Mount Not Appearing

**Check if DFS is enabled:**
```json
{
  "dfs": {
    "enabled": true
  }
}
```

**Verify mount point exists:**
```bash
ls -la /mnt/decypharr/
```

**Check logs:**
```bash
docker logs decypharr | grep -i dfs
```

### Permission Issues

**Fix ownership:**
```bash
# On host
chown -R 1000:1000 /mnt/decypharr/

# In Docker compose
environment:
  - PUID=1000
  - PGID=1000
```

**Enable allow_other:**
```json
{
  "dfs": {
    "allow_other": true,
    "default_permissions": true
  }
}
```

### Slow Streaming

**Check cache size:**
- Increase `disk_cache_size` to 10GB+
- Increase `read_ahead_size` to 32MB+

**Enable smart caching for episodes:**
```json
{
  "dfs": {
    "smart_caching": true
  }
}
```

**Check network:**
- Verify debrid service is responsive
- Check network bandwidth
- Review `max_concurrent_reads`

### High CPU/Memory Usage

**Reduce concurrent reads:**
```json
{
  "dfs": {
    "max_concurrent_reads": 2
  }
}
```

**Reduce cache size:**
```json
{
  "dfs": {
    "disk_cache_size": "5GB"
  }
}
```

### Cache Filling Up

**Increase cleanup frequency:**
```json
{
  "dfs": {
    "cache_cleanup_interval": "2m"
  }
}
```

**Reduce expiry time:**
```json
{
  "dfs": {
    "cache_expiry": "6h"
  }
}
```

### Files Not Showing Up

**Wait for metadata refresh:**
- Torrents may take a few seconds to appear
- Check the debrid service web interface

**Verify torrent status:**
- Ensure torrents are completed/seeding
- Check for errors in the debrid service

**Force refresh:**
- Restart Decypharr to refresh mount state

## Comparison: DFS vs Rclone

| Feature | DFS | Rclone |
|---------|-----|--------|
| **Setup** | Built-in, automatic | Requires external setup |
| **Dependencies** | None (except FUSE) | Requires rclone binary |
| **Performance** | Optimized for debrid | General-purpose |
| **Smart Caching** | Yes (pattern detection) | Limited |
| **Episode Prefetch** | Yes | No |
| **Resource Usage** | Lower | Higher |
| **Configuration** | Simple JSON | Complex rclone config |
| **Stability** | Stable | Very stable |
| **Features** | Media-focused | Feature-rich |

### When to Use DFS

Use DFS when:
- You want simple, automatic setup
- Streaming media is your primary use case
- You want smart caching features
- You prefer lower resource usage
- You're binge-watching TV shows

### When to Use Rclone

Use rclone when:
- You need advanced rclone features (encryption, compression, etc.)
- You're already familiar with rclone
- You need maximum stability
- You use non-debrid cloud storage

## Advanced Configuration

### Per-Debrid Mount Paths

Customize mount paths per debrid:

```json
{
  "debrids": [
    {
      "name": "realdebrid",
      "rclone_mount_path": "/mnt/realdebrid-custom"
    },
    {
      "name": "torbox",
      "rclone_mount_path": "/mnt/torbox-custom"
    }
  ],
  "dfs": {
    "enabled": true
  }
}
```

### Multiple Debrid Accounts

DFS supports multiple debrid services simultaneously:

```json
{
  "debrids": [
    {
      "name": "realdebrid",
      "api_key": "..."
    },
    {
      "name": "torbox",
      "api_key": "..."
    }
  ],
  "dfs": {
    "enabled": true,
    "mount_path": "/mnt/decypharr"
  }
}
```

Access each service at:
- `/mnt/decypharr/realdebrid/`
- `/mnt/decypharr/torbox/`

## Best Practices

1. **Set Appropriate Cache Size**: Allocate 10-20GB for optimal streaming
2. **Enable Smart Caching**: For better performance with TV shows
3. **Use SSD for Cache**: If possible, store cache on SSD for faster access
4. **Monitor Cache Hit Rate**: Check stats page to ensure cache is effective
5. **Adjust Based on Usage**: Tune settings based on your streaming patterns
6. **Regular Cleanup**: Let automatic cleanup handle cache management
7. **Match UID/GID**: Ensure Docker UID/GID matches host user

## Security Considerations

- **File Permissions**: DFS respects `umask` and permission settings
- **User Isolation**: Set appropriate `uid` and `gid`
- **Access Control**: Use `allow_other` carefully in multi-user environments
- **Cache Location**: Ensure cache directory has appropriate permissions

## Next Steps

- Learn about [Smart Caching Features](../features/dfs-smart-caching.md)
- Compare with [Internal Rclone Mounting](internal-mounting.md)
- Check the [Stats Page](http://localhost:8282/stats) for performance metrics
