package vfs

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"

	"github.com/sirrobot01/decypharr/pkg/manager"
)

// StreamingReader provides a ring-buffer based streaming reader for instant playback
// Similar to WebDAV implementation, but integrated with DFS caching
// This is used for sequential reads (video playback) to minimize latency
type StreamingReader struct {
	// Source
	manager     *manager.Manager
	info        *manager.FileInfo
	startOffset int64
	endOffset   int64

	// Ring buffer settings
	chunkSize  int64       // Size of each buffered chunk (512KB for quick startup)
	queueDepth int         // Number of chunks to buffer ahead (4 = 2MB buffer)
	chunkCh    chan []byte // Buffered channel for chunks
	errCh      chan error  // Error channel
	pool       *sync.Pool  // Buffer pool for zero-allocation

	// State
	ctx        context.Context
	cancel     context.CancelFunc
	bytesRead  atomic.Int64
	closed     atomic.Bool
	readerDone atomic.Bool

	// Background caching (optional)
	cacheFile *File // If provided, cache downloaded data in background
}

const (
	streamChunkSize  = 256 * 1024 // 256KB per chunk - optimal for video players
	streamQueueDepth = 8          // Total buffered data = 2MB, more responsive
)

// NewStreamingReader creates a streaming reader with ring buffer
// If cacheFile is provided, data will be cached in background (non-blocking)
func NewStreamingReader(ctx context.Context, mgr *manager.Manager, info *manager.FileInfo,
	startOffset, endOffset int64, cacheFile *File) *StreamingReader {

	ctx, cancel := context.WithCancel(ctx)

	sr := &StreamingReader{
		manager:     mgr,
		info:        info,
		startOffset: startOffset,
		endOffset:   endOffset,
		chunkSize:   streamChunkSize,
		queueDepth:  streamQueueDepth,
		chunkCh:     make(chan []byte, streamQueueDepth),
		errCh:       make(chan error, 1),
		ctx:         ctx,
		cancel:      cancel,
		cacheFile:   cacheFile,
		pool: &sync.Pool{
			New: func() interface{} {
				buf := make([]byte, streamChunkSize)
				return &buf
			},
		},
	}

	// Start background reader
	go sr.readLoop()

	return sr
}

// readLoop continuously reads from remote and fills the ring buffer
func (sr *StreamingReader) readLoop() {
	defer func() {
		sr.readerDone.Store(true)
		close(sr.chunkCh)
	}()

	// Get reader from manager
	rc, err := sr.manager.StreamReader(sr.ctx, sr.info.Parent(), sr.info.Name(),
		sr.startOffset, sr.endOffset)
	if err != nil {
		sr.errCh <- fmt.Errorf("get download link: %w", err)
		return
	}
	defer rc.Close()

	var readErr error
	currentOffset := sr.startOffset
	totalSize := sr.endOffset - sr.startOffset + 1
	if sr.endOffset <= 0 || sr.endOffset >= sr.info.Size() {
		totalSize = sr.info.Size() - sr.startOffset
	}

	for sr.bytesRead.Load() < totalSize {
		// Check if closed
		if sr.closed.Load() {
			readErr = context.Canceled
			break
		}

		// Get buffer from pool
		bufPtr := sr.pool.Get().(*[]byte)
		buf := *bufPtr

		// Read chunk
		n, err := io.ReadFull(rc, buf)
		if err != nil && err != io.EOF && !errors.Is(err, io.ErrUnexpectedEOF) {
			sr.pool.Put(bufPtr)
			readErr = fmt.Errorf("read chunk at offset %d: %w", currentOffset, err)
			break
		}

		if n == 0 {
			sr.pool.Put(bufPtr)
			if err == io.EOF {
				readErr = nil
			} else {
				readErr = err
			}
			break
		}

		// Adjust for end of file
		remaining := totalSize - sr.bytesRead.Load()
		if int64(n) > remaining {
			n = int(remaining)
		}

		// Create a copy for the channel (don't send pooled buffer directly)
		chunk := make([]byte, n)
		copy(chunk, buf[:n])
		sr.pool.Put(bufPtr) // Return to pool immediately

		// Send to channel (blocks if buffer is full - this is the ring buffer backpressure)
		select {
		case sr.chunkCh <- chunk:
			currentOffset += int64(n)
			sr.bytesRead.Add(int64(n))

			// Background cache write (non-blocking, fire-and-forget)
			if sr.cacheFile != nil {
				go func(data []byte, offset int64) {
					_, _ = sr.cacheFile.WriteAt(data, offset)
				}(chunk, currentOffset-int64(n))
			}

		case <-sr.ctx.Done():
			readErr = sr.ctx.Err()
			break
		}

		// Check for EOF
		if err == io.EOF || errors.Is(err, io.ErrUnexpectedEOF) {
			readErr = nil
			break
		}
	}

	// Send any error to error channel
	if readErr != nil && readErr != io.EOF {
		select {
		case sr.errCh <- readErr:
		default:
		}
	}
}

// Read implements io.Reader - reads from the ring buffer
func (sr *StreamingReader) Read(p []byte) (int, error) {
	if sr.closed.Load() {
		return 0, io.EOF
	}

	// Try to read from channel
	select {
	case chunk, ok := <-sr.chunkCh:
		if !ok {
			// Channel closed - check if there was an error
			select {
			case err := <-sr.errCh:
				return 0, err
			default:
				return 0, io.EOF
			}
		}

		// Copy chunk to output buffer
		n := copy(p, chunk)

		// If buffer is too small, this is a short read
		// Return what we copied and let caller handle it
		// This follows io.Reader semantics better than trying to save remainder
		return n, nil

	case err := <-sr.errCh:
		return 0, err

	case <-sr.ctx.Done():
		return 0, sr.ctx.Err()
	}
}

// Close stops the streaming reader
func (sr *StreamingReader) Close() error {
	if sr.closed.CompareAndSwap(false, true) {
		sr.cancel()

		// Drain the channel to unblock reader
		go func() {
			for range sr.chunkCh {
				// Drain
			}
		}()
	}
	return nil
}

// BytesRead returns the total bytes read from remote
func (sr *StreamingReader) BytesRead() int64 {
	return sr.bytesRead.Load()
}

// IsComplete returns true if all data has been read
func (sr *StreamingReader) IsComplete() bool {
	return sr.readerDone.Load()
}

// Removed shouldUseStreaming - we now always use streaming for network reads
// StreamingReader is efficient for both sequential AND random reads because:
// 1. Returns data immediately as chunks arrive (no blocking)
// 2. Background caching continues after first chunk
// 3. Small overhead for random reads is negligible vs instant playback benefit

// streamingReadAt performs a streaming read using the ring buffer
// This is optimized for sequential playback with instant startup
func (f *File) streamingReadAt(ctx context.Context, p []byte, offset int64) (int, error) {
	// Calculate read size
	readSize := int64(len(p))
	if offset+readSize > f.info.Size() {
		readSize = f.info.Size() - offset
	}

	// Create streaming reader
	endOffset := offset + readSize - 1
	sr := NewStreamingReader(ctx, f.manager, f.info, offset, endOffset, f)
	defer func() {
		// Ensure cleanup happens
		_ = sr.Close()
	}()

	// Read all data from stream
	totalRead := 0
	for totalRead < len(p) {
		n, err := sr.Read(p[totalRead:])
		totalRead += n

		if err != nil {
			if errors.Is(err, io.EOF) {
				// EOF is expected when we've read all available data
				if totalRead > 0 {
					return totalRead, nil
				}
				return 0, io.EOF
			}
			// Return actual error with any data read so far
			if totalRead > 0 {
				return totalRead, nil
			}
			return 0, err
		}

		// Check if we've read enough
		if totalRead >= len(p) || totalRead >= int(readSize) {
			break
		}

		// Check context cancellation
		if ctx.Err() != nil {
			if totalRead > 0 {
				return totalRead, nil
			}
			return 0, ctx.Err()
		}
	}

	return totalRead, nil
}
