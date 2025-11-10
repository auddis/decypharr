package manager

import (
	"context"
	"runtime"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sirrobot01/decypharr/internal/utils"
	debrid "github.com/sirrobot01/decypharr/pkg/debrid/common"
	"github.com/sirrobot01/decypharr/pkg/debrid/types"
	"github.com/sirrobot01/decypharr/pkg/storage"
)

// refreshTorrents refreshes torrents from a specific debrid service
func (m *Manager) refreshTorrents(ctx context.Context, debridName string, debridClient debrid.Client) {
	select {
	case <-ctx.Done():
		return
	default:
	}

	defer func() {
		runtime.GC()
		debug.FreeOSMemory()
	}()

	// Use singleflight to prevent concurrent refreshes for the same debrid
	_, err, _ := m.refreshSG.Do(debridName, func() (interface{}, error) {
		m.logger.Debug().Str("debrid", debridName).Msg("Starting torrent refresh")

		// Fetch torrents from this debrid client
		torrents, err := debridClient.GetTorrents()
		if err != nil {
			m.logger.Error().Err(err).Str("debrid", debridName).Msg("Failed to get torrents")
			return nil, err
		}

		if len(torrents) == 0 {
			m.logger.Debug().Str("debrid", debridName).Msg("No torrents found")
			return nil, nil
		}

		m.logger.Debug().Str("debrid", debridName).Msgf("Found %d torrents from debrid", len(torrents))

		// Build map of current torrents by infohash
		currentByInfoHash := make(map[string]*types.Torrent, len(torrents))
		for i := range torrents {
			t := torrents[i]
			if _, exists := currentByInfoHash[t.InfoHash]; exists {
				continue
			}
			currentByInfoHash[t.InfoHash] = t
		}

		// Clear the torrents slice to free memory (we only need the map now)
		torrents = nil

		// Stream through cached torrents to detect changes
		newTorrents := make([]*types.Torrent, 0, 100)
		torrentsToUpdate := make([]*storage.Torrent, 0, 100)
		torrentsToDelete := make([]string, 0, 10)

		// Build a lightweight set of cached infohashes during streaming (memory efficient)
		cachedInfoHashes := make(map[string]bool, len(currentByInfoHash))

		batchSize := 500
		err = m.storage.ForEachBatch(batchSize, func(batch []*storage.Torrent) error {
			for _, cached := range batch {
				// Mark as cached
				cachedInfoHashes[cached.InfoHash] = true

				currentTorrent, existsOnDebrid := currentByInfoHash[cached.InfoHash]
				hasPlacementOnDebrid := cached.HasPlacement(debridName, cached.InfoHash)

				if hasPlacementOnDebrid {
					// Cached torrent has a placement on this debrid
					if !existsOnDebrid {
						cached.RemovePlacement(debridName, nil)

						if len(cached.Placements) == 0 {
							// No placements left - delete entire torrent
							torrentsToDelete = append(torrentsToDelete, cached.InfoHash)
						} else {
							// Still has other placements - update
							torrentsToUpdate = append(torrentsToUpdate, cached)
						}
					}
				} else if existsOnDebrid {
					// Cached torrent doesn't have placement on this debrid, but torrent exists on debrid
					// This is a new placement to add
					newTorrents = append(newTorrents, currentTorrent)
				}
			}
			return nil
		})

		if err != nil {
			m.logger.Error().Err(err).Msg("Failed to stream cached torrents")
			return nil, err
		}

		// Check for brand new torrents (not in cache at all)
		for infohash, t := range currentByInfoHash {
			if !cachedInfoHashes[infohash] {
				newTorrents = append(newTorrents, t)
			}
		}

		// Clear the large map immediately after use to free memory
		for k := range currentByInfoHash {
			delete(currentByInfoHash, k)
		}
		currentByInfoHash = nil

		for k := range cachedInfoHashes {
			delete(cachedInfoHashes, k)
		}
		cachedInfoHashes = nil

		// Handle deletions
		if len(torrentsToDelete) > 0 {
			m.logger.Debug().Str("debrid", debridName).Msgf("Deleting %d torrents with no remaining placements", len(torrentsToDelete))
			for _, infohash := range torrentsToDelete {
				if err := m.storage.Delete(infohash); err != nil {
					m.logger.Error().Err(err).Str("infohash", infohash).Msg("Failed to delete torrent")
				}
			}
		}
		// Clear deletion slice
		torrentsToDelete = nil

		// Batch update torrents with changed placements
		if len(torrentsToUpdate) > 0 {
			m.logger.Debug().Str("debrid", debridName).Msgf("Updating %d torrents", len(torrentsToUpdate))
			if err := m.storage.BatchAddOrUpdate(torrentsToUpdate); err != nil {
				m.logger.Error().Err(err).Msg("Failed to batch update torrents")
			}
		}
		// Clear update slice
		torrentsToUpdate = nil

		// Process new torrents/placements with batching
		if len(newTorrents) > 0 {
			m.logger.Trace().Str("debrid", debridName).Msgf("Found %d new torrents/placements", len(newTorrents))

			workChan := make(chan *types.Torrent, min(100, len(newTorrents)))
			batchChan := make(chan *storage.Torrent, 50)
			var processWg sync.WaitGroup
			var batchWg sync.WaitGroup
			var processed int64

			// Batch writer
			batchWg.Add(1)
			go func() {
				defer batchWg.Done()
				batch := make([]*storage.Torrent, 0, 50)
				ticker := time.NewTicker(3 * time.Second)
				defer ticker.Stop()

				flushBatch := func() {
					if len(batch) > 0 {
						if err := m.storage.BatchAddOrUpdate(batch); err != nil {
							m.logger.Error().Err(err).Msg("Failed to batch write torrents")
						}
						// Clear slice and nil out references to help GC
						for i := range batch {
							batch[i] = nil
						}
						batch = batch[:0]
					}
				}

				for {
					select {
					case t, ok := <-batchChan:
						if !ok {
							flushBatch()
							return
						}
						batch = append(batch, t)
						if len(batch) >= 50 {
							flushBatch()
						}
					case <-ticker.C:
						flushBatch()
					}
				}
			}()

			// Workers
			workers := 10
			for i := 0; i < workers; i++ {
				processWg.Add(1)
				go func() {
					defer processWg.Done()
					for t := range workChan {
						if mt, err := m.processSyncTorrent(t); err != nil {
							m.logger.Error().Err(err).Str("debrid", debridName).Msgf("Failed to process torrent %s", t.Id)
						} else if mt != nil {
							batchChan <- mt
						}
						atomic.AddInt64(&processed, 1)
					}
				}()
			}

			// Send torrents to workers and clear references immediately
			for i, t := range newTorrents {
				workChan <- t
				newTorrents[i] = nil // Clear reference as soon as it's sent
			}
			// Clear the slice itself
			newTorrents = nil

			close(workChan)
			processWg.Wait()
			close(batchChan)
			batchWg.Wait()

			m.logger.Debug().Str("debrid", debridName).Msgf("Processed %d new torrents/placements", processed)
		}

		// Force garbage collection to reclaim memory
		runtime.GC()

		return nil, nil
	})

	if err != nil {
		m.logger.Error().Err(err).Str("debrid", debridName).Msg("Failed to refresh torrents")
	}

	// Refresh entries
	m.RefreshEntries(true)
}

// processSyncTorrent processes a single torrent and returns it for batched writing
func (m *Manager) processSyncTorrent(t *types.Torrent) (*storage.Torrent, error) {
	// Get the debrid client
	client := m.DebridClient(t.Debrid)
	if client == nil {
		return nil, nil
	}

	// Check if files are complete
	if len(t.Files) == 0 || !isComplete(t.Files) {
		if err := client.UpdateTorrent(t); err != nil {
			return nil, err
		}
	}

	if !isComplete(t.Files) {
		m.logger.Debug().
			Str("torrent_id", t.Id).
			Str("torrent_name", t.Name).
			Msg("Torrent still not complete after refresh")
		return nil, nil
	}

	// Parse added timestamp
	addedOn, err := time.Parse(time.RFC3339, t.Added)
	if err != nil {
		addedOn = time.Now()
	}

	// Check if we have an existing managed torrent
	mt, err := m.storage.Get(t.InfoHash)
	if err != nil {
		// Create new managed torrent
		var magnet *utils.Magnet
		if t.Magnet == nil || t.Magnet.Link == "" {
			magnet = utils.ConstructMagnet(t.InfoHash, t.Name)
		} else {
			magnet = t.Magnet
		}
		size := t.Size
		if size == 0 {
			size = t.Bytes
		}
		mt = &storage.Torrent{
			InfoHash:         t.InfoHash,
			Name:             t.Name,
			OriginalFilename: t.OriginalFilename,
			Size:             size,
			Bytes:            size,
			Magnet:           magnet.Link,
			ActiveDebrid:     t.Debrid,
			Placements:       make(map[string]*storage.Placement),
			Files:            make(map[string]*storage.File),
			Status:           t.Status,
			Progress:         t.Progress,
			Speed:            t.Speed,
			Seeders:          t.Seeders,
			IsComplete:       len(t.Files) > 0,
			Bad:              false,
			AddedOn:          addedOn,
			CreatedAt:        addedOn,
			UpdatedAt:        time.Now(),
		}
		mt.Folder = mt.GetFolder()
	}

	// Populate global Files metadata (only if empty)
	if len(mt.Files) == 0 {
		for _, f := range t.GetFiles() {
			mt.Files[f.Name] = &storage.File{
				Name:      f.Name,
				Size:      f.Size,
				IsRar:     f.IsRar,
				ByteRange: f.ByteRange,
				Deleted:   f.Deleted,
				InfoHash:  t.InfoHash,
				Debrid:    t.Debrid,
			}
		}
	}

	// AddOrUpdate or update placement
	placement := mt.AddPlacement(t)
	placement.Progress = t.Progress
	if t.Status == types.TorrentStatusDownloaded {
		downloadedAt := addedOn
		placement.DownloadedAt = &downloadedAt
	}

	// Populate placement-specific file data
	for _, f := range t.GetFiles() {
		placement.Files[f.Name] = &storage.PlacementFile{
			Id:   f.Id,
			Link: f.Link,
			Path: f.Path,
		}
	}

	// If this is the first placement or the only one, make it active
	if mt.ActiveDebrid == "" || len(mt.Placements) == 1 {
		if t.Status == types.TorrentStatusDownloaded {
			_ = mt.ActivatePlacement(t.Debrid)
		}
	}

	return mt, nil
}

// refreshTorrent refreshes a single torrent from its active debrid
func (m *Manager) refreshTorrent(infohash string) (*storage.Torrent, error) {
	torrent, err := m.storage.Get(infohash)
	if err != nil {
		return nil, err
	}

	if torrent.ActiveDebrid == "" {
		return torrent, nil
	}

	client := m.DebridClient(torrent.ActiveDebrid)
	if client == nil {
		return torrent, nil
	}

	placement := torrent.GetActivePlacement(infohash)
	if placement == nil {
		return torrent, nil
	}

	// Get updated torrent info from debrid
	debridTorrent, err := client.GetTorrent(placement.ID)
	if err != nil {
		return nil, err
	}

	// Update placement info
	placement.Status = debridTorrent.Status
	placement.Progress = debridTorrent.Progress

	// Update global Files metadata (only if needed)
	for _, f := range debridTorrent.GetFiles() {
		if _, exists := torrent.Files[f.Name]; !exists {
			torrent.Files[f.Name] = &storage.File{
				Name:      f.Name,
				Size:      f.Size,
				IsRar:     f.IsRar,
				ByteRange: f.ByteRange,
				Deleted:   f.Deleted,
				InfoHash:  debridTorrent.InfoHash,
				Debrid:    debridTorrent.Debrid,
			}
		}
	}

	// Update placement-specific file data
	for _, f := range debridTorrent.GetFiles() {
		placement.Files[f.Name] = &storage.PlacementFile{
			Id:   f.Id,
			Link: f.Link,
			Path: f.Path,
		}
	}

	torrent.Status = debridTorrent.Status
	torrent.Progress = debridTorrent.Progress
	torrent.UpdatedAt = time.Now()

	// Save to storage
	if err := m.AddOrUpdate(torrent, nil); err != nil {
		return nil, err
	}

	return torrent, nil
}

// refreshDebridDownloadLinks refreshes download links for a specific debrid service
func (m *Manager) refreshDebridDownloadLinks(ctx context.Context, debridName string, debridClient debrid.Client) {
	defer func() {
		runtime.GC()
		debug.FreeOSMemory()
	}()

	select {
	case <-ctx.Done():
		return
	default:
	}

	if debridClient == nil {
		m.logger.Warn().Str("debrid", debridName).Msg("Debrid client is nil, skipping download link refresh")
		return
	}

	if err := debridClient.RefreshDownloadLinks(); err != nil {
		m.logger.Error().Err(err).Str("debrid", debridName).Msg("Failed to refresh download links")
	} else {
		m.logger.Debug().Str("debrid", debridName).Msg("Refreshed download links")
	}
}

// isComplete checks if all files in a torrent have download links
func isComplete(files map[string]types.File) bool {
	if len(files) == 0 {
		return false
	}
	for _, file := range files {
		if file.Link == "" {
			return false
		}
	}
	return true
}
