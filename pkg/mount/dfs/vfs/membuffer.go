package vfs

import (
	"container/list"
	"context"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

// MemoryBuffer provides buffering with async disk flushing
// Architecture: Network → Memory → Async Disk Flush
type MemoryBuffer struct {
	// Configuration
	maxSize    int64 // Maximum memory to use
	bufferSize int64 // Size of each buffer chunk

	// Memory storage (fast lookup)
	chunks    map[int64]*BufferChunk // offset -> chunk
	chunksMu  sync.RWMutex
	chunkList *list.List // LRU list for eviction
	totalSize atomic.Int64

	// Async flusher
	flusher     *AsyncFlusher
	flushQueue  chan *BufferChunk
	closeCtx    context.Context
	closeCancel context.CancelFunc

	// Statistics
	stats *MemBufferStats
}

// BufferChunk represents a chunk of data in memory
type BufferChunk struct {
	offset     int64
	data       []byte
	dirty      atomic.Bool   // Needs to be flushed to disk
	accessed   atomic.Int64  // Last access time (Unix nano)
	lruElem    *list.Element // Position in LRU list
	flushing   atomic.Bool   // Currently being flushed
	flushDone  chan struct{} // Signals flush completion
	mu         sync.RWMutex  // Protects data modifications
	pinned     atomic.Bool   // Prevents eviction (dirty chunks being flushed)
}

// MemBufferStats tracks memory buffer performance
type MemBufferStats struct {
	Hits       atomic.Int64
	Misses     atomic.Int64
	Evictions  atomic.Int64
	Flushes    atomic.Int64
	FlushBytes atomic.Int64
	MemoryUsed atomic.Int64
}

// NewMemoryBuffer creates a new memory buffer
func NewMemoryBuffer(ctx context.Context, maxSize, bufferSize int64) *MemoryBuffer {
	closeCtx, cancel := context.WithCancel(ctx)

	mb := &MemoryBuffer{
		maxSize:     maxSize,
		bufferSize:  bufferSize,
		chunks:      make(map[int64]*BufferChunk),
		chunkList:   list.New(),
		flushQueue:  make(chan *BufferChunk, 256),
		closeCtx:    closeCtx,
		closeCancel: cancel,
		stats:       &MemBufferStats{},
	}

	return mb
}

// updateLRU moves chunk to front of LRU list
func (mb *MemoryBuffer) updateLRU(chunk *BufferChunk) {
	mb.chunksMu.Lock()
	defer mb.chunksMu.Unlock()

	if chunk.lruElem != nil {
		mb.chunkList.MoveToFront(chunk.lruElem)
	}
}

// AttachFile attaches a file for async flushing
func (mb *MemoryBuffer) AttachFile(file *os.File) {
	if file == nil && mb.flusher == nil {
		return
	}

	if mb.flusher == nil {
		mb.flusher = NewAsyncFlusher(mb.closeCtx, file, mb.flushQueue)
		go mb.flusher.Run()
		return
	}

	mb.flusher.UpdateFile(file)
}

// Get retrieves data from memory if available
// Returns (data, found)
func (mb *MemoryBuffer) Get(offset, size int64) ([]byte, bool) {
	mb.chunksMu.RLock()

	// Check if we can serve this from a single chunk
	chunk, exists := mb.chunks[offset]
	if exists && int64(len(chunk.data)) >= size {
		chunk.mu.RLock()
		data := chunk.data
		chunk.mu.RUnlock()

		// Update access time
		chunk.accessed.Store(time.Now().UnixNano())
		mb.chunksMu.RUnlock()

		// Update LRU separately (no double-locking)
		mb.updateLRU(chunk)

		mb.stats.Hits.Add(1)

		// Return copy to avoid data races
		result := make([]byte, size)
		copy(result, data[:size])
		return result, true
	}

	// Try multi-chunk read
	if exists {
		result := make([]byte, 0, size)
		currentOffset := offset
		remaining := size
		chunksToUpdate := make([]*BufferChunk, 0, 4)

		for remaining > 0 {
			chunk, exists := mb.chunks[currentOffset]
			if !exists {
				mb.chunksMu.RUnlock()
				mb.stats.Misses.Add(1)
				return nil, false
			}

			chunk.mu.RLock()
			chunkData := chunk.data
			chunk.mu.RUnlock()

			// How much can we read from this chunk?
			offsetInChunk := currentOffset - chunk.offset
			availableInChunk := int64(len(chunkData)) - offsetInChunk

			if availableInChunk <= 0 {
				mb.chunksMu.RUnlock()
				mb.stats.Misses.Add(1)
				return nil, false
			}

			toRead := remaining
			if toRead > availableInChunk {
				toRead = availableInChunk
			}

			result = append(result, chunkData[offsetInChunk:offsetInChunk+toRead]...)
			currentOffset += toRead
			remaining -= toRead

			// Update access time and track for LRU update
			chunk.accessed.Store(time.Now().UnixNano())
			chunksToUpdate = append(chunksToUpdate, chunk)
		}

		mb.chunksMu.RUnlock()

		// Update LRU for all accessed chunks
		for _, chunk := range chunksToUpdate {
			mb.updateLRU(chunk)
		}

		mb.stats.Hits.Add(1)
		return result, true
	}

	mb.chunksMu.RUnlock()
	mb.stats.Misses.Add(1)
	return nil, false
}

// Put stores data in memory and marks for async flushing
func (mb *MemoryBuffer) Put(offset int64, data []byte) error {
	dataSize := int64(len(data))
	if dataSize == 0 {
		return nil
	}

	// Lock early to prevent races
	mb.chunksMu.Lock()
	defer mb.chunksMu.Unlock()

	// Calculate space needed
	additionalNeeded := dataSize
	if existing, ok := mb.chunks[offset]; ok {
		diff := dataSize - int64(len(existing.data))
		if diff > 0 {
			additionalNeeded = diff
		} else {
			additionalNeeded = 0
		}
	}

	// Evict if necessary to make space (while holding lock)
	for additionalNeeded > 0 && mb.totalSize.Load()+additionalNeeded > mb.maxSize {
		if !mb.evictLRULocked() {
			// Can't evict anymore, fail
			return fmt.Errorf("memory buffer full, cannot evict")
		}
	}

	// Now we have guaranteed space
	if existing, ok := mb.chunks[offset]; ok {
		now := time.Now().UnixNano()
		oldSize := int64(len(existing.data))

		// Reallocate only if size changed
		if oldSize != dataSize {
			existing.data = make([]byte, dataSize)
			mb.totalSize.Add(dataSize - oldSize)
		}

		copy(existing.data, data)
		existing.accessed.Store(now)
		existing.dirty.Store(true)
		existing.pinned.Store(true) // Pin while dirty to prevent premature eviction

		if existing.lruElem != nil {
			mb.chunkList.MoveToFront(existing.lruElem)
		} else {
			existing.lruElem = mb.chunkList.PushFront(offset)
		}

		mb.stats.MemoryUsed.Store(mb.totalSize.Load())

		// Queue for async flush
		select {
		case mb.flushQueue <- existing:
		default:
			// Queue full - chunk will be flushed on next opportunity or Close()
		}

		return nil
	}

	// Create new chunk
	chunk := &BufferChunk{
		offset:    offset,
		data:      make([]byte, dataSize),
		accessed:  atomic.Int64{},
		flushDone: make(chan struct{}),
	}
	copy(chunk.data, data)
	chunk.accessed.Store(time.Now().UnixNano())
	chunk.dirty.Store(true)
	chunk.pinned.Store(true) // Pin while dirty

	// Add to map and LRU list
	mb.chunks[offset] = chunk
	chunk.lruElem = mb.chunkList.PushFront(offset)

	// Update total size
	mb.totalSize.Add(dataSize)
	mb.stats.MemoryUsed.Store(mb.totalSize.Load())

	// Queue for async flush
	select {
	case mb.flushQueue <- chunk:
		// Queued successfully
	default:
		// Queue full - chunk will be flushed on next opportunity or Close()
	}

	return nil
}

// evictLRULocked evicts the least recently used chunk (must be called with lock held)
func (mb *MemoryBuffer) evictLRULocked() bool {
	// Scan from back of LRU list to find an evictable chunk
	elem := mb.chunkList.Back()
	for elem != nil {
		offset := elem.Value.(int64)
		chunk, exists := mb.chunks[offset]

		if !exists {
			// Inconsistent state, remove from list and continue
			next := elem.Prev()
			mb.chunkList.Remove(elem)
			elem = next
			continue
		}

		// Skip pinned chunks (dirty chunks being flushed)
		if chunk.pinned.Load() {
			elem = elem.Prev()
			continue
		}

		// Found evictable chunk
		// If it's dirty but not pinned, wait for flush to complete
		if chunk.dirty.Load() {
			// Try to wait briefly for flush
			select {
			case <-chunk.flushDone:
				// Flush completed
			default:
				// Still flushing, skip this one
				elem = elem.Prev()
				continue
			}
		}

		// Remove from map and list
		delete(mb.chunks, offset)
		mb.chunkList.Remove(elem)

		// Update total size
		chunkSize := int64(len(chunk.data))
		mb.totalSize.Add(-chunkSize)
		mb.stats.MemoryUsed.Store(mb.totalSize.Load())
		mb.stats.Evictions.Add(1)

		return true
	}

	return false // No evictable chunks found
}

// Flush flushes all dirty chunks to disk (blocking)
func (mb *MemoryBuffer) Flush() error {
	mb.chunksMu.RLock()
	dirtyChunks := make([]*BufferChunk, 0)
	for _, chunk := range mb.chunks {
		if chunk.dirty.Load() {
			dirtyChunks = append(dirtyChunks, chunk)
		}
	}
	mb.chunksMu.RUnlock()

	// Queue all dirty chunks for flushing
	for _, chunk := range dirtyChunks {
		select {
		case mb.flushQueue <- chunk:
			// Queued
		case <-mb.closeCtx.Done():
			return mb.closeCtx.Err()
		}
	}

	// Wait for all dirty chunks to be flushed
	timeout := time.After(30 * time.Second)
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-timeout:
			return fmt.Errorf("flush timeout: some chunks still dirty")
		case <-ticker.C:
			allClean := true
			for _, chunk := range dirtyChunks {
				if chunk.dirty.Load() {
					allClean = false
					break
				}
			}
			if allClean {
				return nil
			}
		case <-mb.closeCtx.Done():
			return mb.closeCtx.Err()
		}
	}
}

// Close closes the memory buffer and flushes pending data
func (mb *MemoryBuffer) Close() error {
	// Flush all pending data
	_ = mb.Flush()

	// Close flusher
	mb.closeCancel()

	// Wait for flusher to finish
	if mb.flusher != nil {
		mb.flusher.Wait()
	}

	// Clear memory
	mb.chunksMu.Lock()
	mb.chunks = make(map[int64]*BufferChunk)
	mb.chunkList = list.New()
	mb.totalSize.Store(0)
	mb.chunksMu.Unlock()

	return nil
}

// GetStats returns buffer statistics
func (mb *MemoryBuffer) GetStats() map[string]interface{} {
	hitRate := 0.0
	total := mb.stats.Hits.Load() + mb.stats.Misses.Load()
	if total > 0 {
		hitRate = float64(mb.stats.Hits.Load()) / float64(total) * 100.0
	}

	return map[string]interface{}{
		"hits":         mb.stats.Hits.Load(),
		"misses":       mb.stats.Misses.Load(),
		"hit_rate_pct": hitRate,
		"evictions":    mb.stats.Evictions.Load(),
		"flushes":      mb.stats.Flushes.Load(),
		"flush_bytes":  mb.stats.FlushBytes.Load(),
		"memory_used":  mb.stats.MemoryUsed.Load(),
		"memory_limit": mb.maxSize,
		"chunks_count": mb.chunkList.Len(),
	}
}

// AsyncFlusher flushes dirty buffers to disk in the background
type AsyncFlusher struct {
	ctx        context.Context
	file       *os.File
	flushQueue chan *BufferChunk
	wg         sync.WaitGroup
	mu         sync.RWMutex
}

// NewAsyncFlusher creates a new async flusher
func NewAsyncFlusher(ctx context.Context, file *os.File, flushQueue chan *BufferChunk) *AsyncFlusher {
	return &AsyncFlusher{
		ctx:        ctx,
		file:       file,
		flushQueue: flushQueue,
	}
}

// Run runs the async flusher
func (af *AsyncFlusher) Run() {
	af.wg.Add(1)
	defer af.wg.Done()

	for {
		select {
		case chunk := <-af.flushQueue:
			if chunk == nil {
				continue
			}

			// Skip if already clean or being flushed
			if !chunk.dirty.Load() || !chunk.flushing.CompareAndSwap(false, true) {
				continue
			}

			// Flush to disk
			chunk.mu.RLock()
			data := chunk.data
			offset := chunk.offset
			chunk.mu.RUnlock()

			af.mu.RLock()
			file := af.file
			af.mu.RUnlock()

			if file != nil {
				n, err := file.WriteAt(data, offset)
				if err == nil && n == len(data) {
					// Successfully flushed
					chunk.dirty.Store(false)
					chunk.pinned.Store(false) // Unpin after successful flush

					// Signal flush completion
					select {
					case <-chunk.flushDone:
						// Already closed
					default:
						close(chunk.flushDone)
					}
				}
				// On error, mark as not flushing so it can be retried
			}

			chunk.flushing.Store(false)

		case <-af.ctx.Done():
			// Drain remaining chunks before exit
			for {
				select {
				case chunk := <-af.flushQueue:
					if chunk != nil && chunk.dirty.Load() {
						af.mu.RLock()
						file := af.file
						af.mu.RUnlock()

						if file != nil {
							chunk.mu.RLock()
							n, err := file.WriteAt(chunk.data, chunk.offset)
							chunk.mu.RUnlock()

							if err == nil && n == len(chunk.data) {
								chunk.dirty.Store(false)
								chunk.pinned.Store(false)

								select {
								case <-chunk.flushDone:
								default:
									close(chunk.flushDone)
								}
							}
						}
						chunk.flushing.Store(false)
					}
				default:
					return
				}
			}
		}
	}
}

// Wait waits for the flusher to finish
func (af *AsyncFlusher) Wait() {
	af.wg.Wait()
}

// UpdateFile swaps the file handle used by the async flusher.
func (af *AsyncFlusher) UpdateFile(file *os.File) {
	af.mu.Lock()
	af.file = file
	af.mu.Unlock()
}

// Note: DownloadToMemory was removed - all network reads now use StreamingReader
// which provides instant playback startup and progressive downloading.
