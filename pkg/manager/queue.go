package manager

import (
	"cmp"
	"context"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/logger"
	"github.com/sirrobot01/decypharr/internal/utils"
	"github.com/sirrobot01/decypharr/pkg/arr"
	debridTypes "github.com/sirrobot01/decypharr/pkg/debrid/types"
	"github.com/sirrobot01/decypharr/pkg/storage"
)

type ImportType string

const (
	ImportTypeQBit    ImportType = "qbit"
	ImportTypeAPI     ImportType = "api"
	ImportTypeSABnzbd ImportType = "sabnzbd"
	ImportSwitcher    ImportType = "switcher"
)

type ImportRequest struct {
	Name             string                `json:"name"`
	NZBContent       []byte                `json:"-,omitempty"`
	Id               string                `json:"id"`
	DownloadFolder   string                `json:"downloadFolder"`
	SelectedDebrid   string                `json:"debrid"`
	Magnet           *utils.Magnet         `json:"magnet"`
	Arr              *arr.Arr              `json:"arr"`
	Action           config.DownloadAction `json:"action"`
	DownloadUncached *bool                 `json:"downloadUncached"`
	CallBackUrl      string                `json:"callBackUrl"`
	SkipMultiSeason  bool                  `json:"skip_multi_season"`

	Status      string    `json:"status"`
	CompletedAt time.Time `json:"completedAt,omitempty"`
	Error       string    `json:"error,omitempty"`

	Type  ImportType `json:"type"`
	Async bool       `json:"async"`
}

func NewTorrentRequest(debrid string, downloadFolder string, magnet *utils.Magnet, arr *arr.Arr, action config.DownloadAction, downloadUncached *bool, callBackUrl string, importType ImportType, skipMultiSeason bool) *ImportRequest {

	return &ImportRequest{
		Id:               uuid.New().String(),
		Status:           "started",
		DownloadFolder:   downloadFolder,
		SelectedDebrid:   cmp.Or(arr.SelectedDebrid, debrid), // Use debrid from arr if available
		Magnet:           magnet,
		Arr:              arr,
		Action:           action,
		DownloadUncached: downloadUncached,
		CallBackUrl:      callBackUrl,
		Type:             importType,
		SkipMultiSeason:  skipMultiSeason,
	}
}

func NewNZBRequest(name, downloadFolder string, nzbContent []byte, arr *arr.Arr, action config.DownloadAction, callBackUrl string, importType ImportType, skipMultiSeason bool) *ImportRequest {
	return &ImportRequest{
		Name:            name,
		Id:              uuid.New().String(),
		Status:          "started",
		DownloadFolder:  downloadFolder,
		SelectedDebrid:  "usenet", // NZB imports always use usenet
		NZBContent:      nzbContent,
		Arr:             arr,
		Action:          action,
		CallBackUrl:     callBackUrl,
		Type:            importType,
		SkipMultiSeason: skipMultiSeason,
	}
}

// Queue manages persistent entry storage for the download queue.
// Both qBittorrent and SABnzbd API frontends read from this.
type Queue struct {
	storage            *storage.Storage
	logger             zerolog.Logger
	removeStalledAfter time.Duration
	mu                 sync.RWMutex
	ctx                context.Context
	cancel             context.CancelFunc
}

func newQueue(ctx context.Context, storage *storage.Storage, removeStalledAfterStr string) *Queue {
	ctx, cancel := context.WithCancel(ctx)
	q := &Queue{
		storage: storage,
		ctx:     ctx,
		cancel:  cancel,
		logger:  logger.New("queue"),
	}

	if removeStalledAfterStr != "" {
		removeStalledAfter, err := utils.ParseDuration(removeStalledAfterStr)
		if err == nil {
			q.removeStalledAfter = removeStalledAfter
		}
	}

	return q
}

func (q *Queue) Add(torrent *storage.Entry) error {
	return q.storage.AddQueue(torrent)
}

func (q *Queue) GetTorrent(infohash string) (*storage.Entry, error) {
	return q.storage.GetQueued(infohash)
}

func (q *Queue) Delete(infohash string, cleanup func(t *storage.Entry) error) error {
	// Wrap the cleanup function to ensure we always delete the entry files
	deleteFile := func(entry *storage.Entry) {
		// Delete the downloaded path if it exists
		downloadedPath := entry.DownloadPath()
		if downloadedPath != "" {
			if err := os.RemoveAll(downloadedPath); err != nil {
				q.logger.Error().Err(err).Str("path", downloadedPath).Msg("Failed to delete downloaded file")
			}
		}
	}
	finalCleanup := func(entry *storage.Entry) error {
		deleteFile(entry)
		if cleanup != nil {
			return cleanup(entry)
		}
		return nil
	}
	return q.storage.DeleteQueued(infohash, finalCleanup)
}

func (q *Queue) DeleteWhere(category string, protocol config.Protocol, state storage.TorrentState, hashes []string, cleanup func(t *storage.Entry) error) error {
	return q.storage.DeleteWhereQueued(q.ListFilterFunc(category, protocol, state, hashes), cleanup)
}

func (q *Queue) DeleteStalled() error {
	cutoff := time.Now().Add(-q.removeStalledAfter)
	return q.storage.DeleteWhereQueued(func(t *storage.Entry) bool {
		if !t.AddedOn.Before(cutoff) {
			return false
		}
		// Torrent entries: not downloading, no seeders, no progress
		if t.Status != debridTypes.TorrentStatusDownloading && t.Seeders == 0 && t.Progress == 0 {
			return true
		}
		// NZB entries stuck in error state with no progress
		if t.State == storage.EntryStateError && t.Progress == 0 {
			return true
		}
		return false
	}, nil)
}

func (q *Queue) Update(torrent *storage.Entry) error {
	// Update the state here
	return q.storage.UpdateQueue(torrent)
}

func (q *Queue) ListFilterFunc(category string, protocol config.Protocol, state storage.TorrentState, hashes []string) func(*storage.Entry) bool {
	hashSet := make(map[string]struct{}, len(hashes))
	if len(hashes) > 0 {
		for _, h := range hashes {
			hashSet[h] = struct{}{}
		}
	}

	var filterFunc func(*storage.Entry) bool
	if category != "" || len(hashes) != 0 || state != "" || protocol != config.ProtocolAll {
		filterFunc = func(t *storage.Entry) bool {
			if category != "" && t.Category != category {
				return false
			}
			if state != "" && t.State != state {
				return false
			}
			if len(hashSet) > 0 {
				if _, ok := hashSet[t.InfoHash]; !ok {
					return false
				}
			}
			if protocol != config.ProtocolAll && t.Protocol != protocol {
				return false
			}
			return true
		}
	}
	return filterFunc
}

func (q *Queue) ListFilter(category string, protocol config.Protocol, state storage.TorrentState, hashes []string, sortBy string, reverse bool) []*storage.Entry {
	filterFunc := q.ListFilterFunc(category, protocol, state, hashes)
	torrents, err := q.storage.FilterQueued(filterFunc)
	if err != nil {
		// return empty list on error
		return []*storage.Entry{}
	}

	if sortBy != "" {
		sort.Slice(torrents, func(i, j int) bool {
			// If ascending is false, swap i and j to get descending order
			if !reverse {
				i, j = j, i
			}

			switch sortBy {
			case "name":
				return torrents[i].Name < torrents[j].Name
			case "size":
				return torrents[i].Size < torrents[j].Size
			case "added_on":
				return torrents[i].AddedOn.Before(torrents[j].AddedOn)
			case "completed", "downloaded":
				return torrents[i].CompletedAt.Before(*torrents[j].CompletedAt)
			case "progress":
				return torrents[i].Progress < torrents[j].Progress
			case "category":
				return torrents[i].Category < torrents[j].Category
			case "seeders":
				return torrents[i].Seeders < torrents[j].Seeders
			default:
				// Default sort by added_on
				return torrents[i].AddedOn.Before(torrents[j].AddedOn)
			}
		})
	}
	return torrents
}

func (q *Queue) UpdateWhere(predicate func(*storage.Entry) bool, updateFunc func(*storage.Entry) bool) error {
	return q.storage.UpdateWhereQueued(predicate, updateFunc)
}

func (q *Queue) Close() {
	q.cancel()
}
