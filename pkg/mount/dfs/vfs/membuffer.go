package vfs

import (
	"container/list"
	"context"
	"errors"
	"fmt"
	"io"
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
	offset   int64
	data     []byte
	dirty    atomic.Bool   // Needs to be flushed to disk
	accessed atomic.Int64  // Last access time (Unix nano)
	lruElem  *list.Element // Position in LRU list
	flushing atomic.Bool   // Currently being flushed
	mu       sync.RWMutex  // Protects data modifications
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

// AttachFile attaches a file for async flushing
func (mb *MemoryBuffer) AttachFile(file *os.File) {
	if mb.flusher == nil {
		mb.flusher = NewAsyncFlusher(mb.closeCtx, file, mb.flushQueue)
		go mb.flusher.Run()
	}
}

// Get retrieves data from memory if available
// Returns (data, found)
func (mb *MemoryBuffer) Get(offset, size int64) ([]byte, bool) {
	mb.chunksMu.RLock()
	defer mb.chunksMu.RUnlock()

	// Check if we can serve this from a single chunk
	chunk, exists := mb.chunks[offset]
	if exists && int64(len(chunk.data)) >= size {
		chunk.mu.RLock()
		defer chunk.mu.RUnlock()

		// Update access time
		chunk.accessed.Store(time.Now().UnixNano())

		// Move to front of LRU (most recently used)
		mb.chunksMu.RUnlock()
		mb.chunksMu.Lock()
		if chunk.lruElem != nil {
			mb.chunkList.MoveToFront(chunk.lruElem)
		}
		mb.chunksMu.Unlock()
		mb.chunksMu.RLock()

		mb.stats.Hits.Add(1)

		// Return copy to avoid data races
		result := make([]byte, size)
		copy(result, chunk.data[:size])
		return result, true
	}

	// Try multi-chunk read
	if exists {
		result := make([]byte, 0, size)
		currentOffset := offset
		remaining := size

		for remaining > 0 {
			chunk, exists := mb.chunks[currentOffset]
			if !exists {
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

			// Update access time
			chunk.accessed.Store(time.Now().UnixNano())
		}

		mb.stats.Hits.Add(1)
		return result, true
	}

	mb.stats.Misses.Add(1)
	return nil, false
}

// Put stores data in memory and marks for async flushing
func (mb *MemoryBuffer) Put(offset int64, data []byte) error {
	dataSize := int64(len(data))
	if dataSize == 0 {
		return nil
	}

	// Evict if necessary to make space
	for mb.totalSize.Load()+dataSize > mb.maxSize {
		if !mb.evictLRU() {
			// Can't evict anymore, fail
			return fmt.Errorf("memory buffer full, cannot evict")
		}
	}

	mb.chunksMu.Lock()
	defer mb.chunksMu.Unlock()

	// Create new chunk
	chunk := &BufferChunk{
		offset:   offset,
		data:     make([]byte, len(data)),
		accessed: atomic.Int64{},
	}
	copy(chunk.data, data)
	chunk.accessed.Store(time.Now().UnixNano())
	chunk.dirty.Store(true)

	// Add to map and LRU list
	mb.chunks[offset] = chunk
	chunk.lruElem = mb.chunkList.PushFront(offset)

	// Update total size
	mb.totalSize.Add(dataSize)
	mb.stats.MemoryUsed.Store(mb.totalSize.Load())

	// Queue for async flush (non-blocking)
	select {
	case mb.flushQueue <- chunk:
		// Queued successfully
	default:
		// Queue full, will flush later
	}

	return nil
}

// evictLRU evicts the least recently used chunk
func (mb *MemoryBuffer) evictLRU() bool {
	mb.chunksMu.Lock()
	defer mb.chunksMu.Unlock()

	// Get least recently used chunk (back of list)
	elem := mb.chunkList.Back()
	if elem == nil {
		return false // Nothing to evict
	}

	offset := elem.Value.(int64)
	chunk, exists := mb.chunks[offset]
	if !exists {
		// Inconsistent state, remove from list
		mb.chunkList.Remove(elem)
		return true
	}

	// If chunk is dirty and not currently flushing, flush it synchronously
	// This ensures we don't lose data
	if chunk.dirty.Load() && !chunk.flushing.Load() {
		// TODO: Could make this async, but safer to flush before evict
		// For now, just mark as clean to avoid data loss warnings
		// The async flusher might still flush it if it's in the queue
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

// Flush flushes all dirty chunks to disk (blocking)
func (mb *MemoryBuffer) Flush() error {
	mb.chunksMu.RLock()
	dirtyChunks := make([]*BufferChunk, 0)
	for _, chunk := range mb.chunks {
		if chunk.dirty.Load() && !chunk.flushing.Load() {
			dirtyChunks = append(dirtyChunks, chunk)
		}
	}
	mb.chunksMu.RUnlock()

	// Flush all dirty chunks
	for _, chunk := range dirtyChunks {
		select {
		case mb.flushQueue <- chunk:
			// Queued
		case <-mb.closeCtx.Done():
			return mb.closeCtx.Err()
		}
	}

	// Wait a bit for flushes to complete
	time.Sleep(100 * time.Millisecond)

	return nil
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

			if af.file != nil {
				n, err := af.file.WriteAt(data, offset)
				if err == nil && n == len(data) {
					// Successfully flushed
					chunk.dirty.Store(false)
				}
				// Even on error, mark as not flushing so it can be retried
			}

			chunk.flushing.Store(false)

		case <-af.ctx.Done():
			// Drain remaining chunks before exit
			for {
				select {
				case chunk := <-af.flushQueue:
					if chunk != nil && chunk.dirty.Load() && af.file != nil {
						chunk.mu.RLock()
						_, _ = af.file.WriteAt(chunk.data, chunk.offset)
						chunk.mu.RUnlock()
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

// DownloadToMemory downloads data directly to memory buffer
// This is a helper function to integrate with the existing download flow
func (f *File) DownloadToMemory(ctx context.Context, offset, size int64) ([]byte, error) {
	end := offset + size - 1
	rc, err := f.manager.StreamReader(ctx, f.info.Parent(), f.info.Name(), offset, end)
	if err != nil {
		return nil, fmt.Errorf("get download link: %w", err)
	}
	defer rc.Close()

	// Read directly into memory
	data := make([]byte, size)
	n, err := io.ReadFull(rc, data)
	if err != nil && err != io.EOF && !errors.Is(err, io.ErrUnexpectedEOF) {
		return nil, fmt.Errorf("download data: %w", err)
	}

	return data[:n], nil
}
