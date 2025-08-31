// Kiwi parts are stored as multiple blocks in a single file. Each block is a protobuf message prefixed by
// its size as a fixed 8-byte little-endian integer. Multiple blocks are concatenated together to form a complete file.
// This file provides utilities to read and write these blocks efficiently, with support for buffering and caching.

package storage

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"runtime"
	"sync"

	"github.com/nobletooth/kiwi/pkg/utils"
	kiwipb "github.com/nobletooth/kiwi/proto"
	"google.golang.org/protobuf/proto"
)

// defaultBufferSize matches the typical OS page size to reduce the number of sys calls.
const defaultBufferSize = 4096

var (
	// bufferPool allows reusing buffers both in BlockReader & BlockWriter to reduce allocations.
	bufferPool = sync.Pool{New: func() any { return bytes.NewBuffer(make([]byte, 0, defaultBufferSize)) }}

	// initCacheOnce ensures cache is reused across multiple SSTables.
	initCacheOnce sync.Once
	sharedCache   *BlockCache
)

// dbCacheKey is the cache key for a data block in the BlockCache.
type dbCacheKey struct {
	table     int64
	ssTableId int64
	offset    int64
}

// BlockCache is an in-memory cache that reduces disk reads for frequently accessed data blocks.
// TODO: Make this an actual cache.
type BlockCache struct {
	mux  sync.Mutex
	data map[dbCacheKey]*kiwipb.DataBlock
}

// getSharedCache returns the singleton shared block cache instance.
func getSharedCache() *BlockCache {
	initCacheOnce.Do(func() {
		sharedCache = &BlockCache{mux: sync.Mutex{}, data: make(map[dbCacheKey]*kiwipb.DataBlock)}
	})
	return sharedCache
}

func (p *BlockCache) Get(table, ssTableId, offset int64) (*kiwipb.DataBlock, bool) {
	p.mux.Lock()
	defer p.mux.Unlock()
	val, exists := p.data[dbCacheKey{table: table, ssTableId: ssTableId, offset: offset}]
	return val, exists
}

func (p *BlockCache) Set(table, ssTableId, offset int64, block *kiwipb.DataBlock) {
	p.mux.Lock()
	defer p.mux.Unlock()
	p.data[dbCacheKey{table: table, ssTableId: ssTableId, offset: offset}] = block
}

// getBlockSize calculates the size of a protobuf message when stored on disk as a block.
func getBlockSize(block proto.Message) int64 {
	return int64(proto.Size(block) + 8)
}

// BlockWriter allows writing protobuf blocks to a block file.
type BlockWriter struct {
	mux    sync.Mutex // Protects the buffer and closed flag.
	closed bool
	writer io.WriteCloser
	buffer *bytes.Buffer
}

// NewBlockWriter is the constructor for BlockWriter.
func NewBlockWriter(writer io.WriteCloser) (*BlockWriter, error) {
	if writer == nil {
		return nil, errors.New("expected non-nil writer")
	}
	bw := &BlockWriter{mux: sync.Mutex{}, writer: writer, closed: false}
	// Call Close when the object is garbage collected.
	runtime.SetFinalizer(bw, func(bw *BlockWriter) { _ = bw.Close() })
	return bw, nil
}

func (bw *BlockWriter) writeBytes(p []byte) (flushed int, err error) {
	if len(p) == 0 {
		return 0, nil
	}

	bw.mux.Lock()
	defer bw.mux.Unlock()
	if bw.closed {
		return 0, errors.New("block writer is closed")
	}
	if bw.buffer == nil { // Take a buffer from the pool.
		bw.buffer = bufferPool.Get().(*bytes.Buffer)
	}

	flushed = 0
	toFlush := len(p)
	for toFlush > 0 {
		if availableBytes := bw.buffer.Available(); availableBytes < toFlush {
			bw.buffer.Write(p[flushed : flushed+availableBytes])
			flushed += availableBytes
			toFlush -= availableBytes
			// Flush the entire buffer.
			if _, err := bw.writer.Write(bw.buffer.Bytes()); err != nil {
				return flushed, err
			}
			bw.buffer.Reset()
		} else {
			bw.buffer.Write(p[flushed:]) // writeBytes all remaining bytes.
			flushed += toFlush
			toFlush = 0
		}
	}

	if flushed != len(p) {
		utils.RaiseInvariant("block", "incomplete_write", "Did an incomplete writeBytes to the block writer buffer.",
			"expected", len(p), "actual", flushed)
		return flushed, fmt.Errorf("incomplete writeBytes to buffer: expected %d bytes, got %d bytes", len(p), flushed)
	}

	return flushed, nil
}

// WriteBlock writes a proto.Message block with its size to the underlying writer.
func (bw *BlockWriter) WriteBlock(msg proto.Message) error {
	if msg == nil {
		return errors.New("cannot create block from nil proto message")
	}

	block, err := proto.Marshal(msg)
	if err != nil {
		return fmt.Errorf("failed to marshal message: %w", err)
	}

	// For each block, writeBytes its size as a fixed 8-byte little-endian integer followed by the block data.
	// This allows the reader to know how many bytes to read for each block.
	blockSizeBinary := make([]byte, 8)
	binary.LittleEndian.PutUint64(blockSizeBinary, uint64(len(block)))
	if _, err := bw.writeBytes(blockSizeBinary); err != nil {
		return fmt.Errorf("failed to writeBytes block size: %w", err)
	}
	if _, err := bw.writeBytes(block); err != nil {
		return fmt.Errorf("failed to writeBytes block data: %w", err)
	}

	return nil
}

func (bw *BlockWriter) Close() error {
	bw.mux.Lock()
	defer func() {
		// Give back the buffer to the pool.
		if bw.buffer != nil {
			bw.buffer.Reset()
			bufferPool.Put(bw.buffer)
			bw.buffer = nil
		}
		bw.closed = true
		bw.mux.Unlock()
	}()

	if bw.closed {
		return errors.New("block writer is already closed")
	}

	// Flush any remaining bytes in the buffer.
	if remaining := bw.buffer.Bytes(); len(remaining) > 0 {
		if _, err := bw.writer.Write(remaining); err != nil {
			return err
		}
	}

	// Close the underlying writer.
	if err := bw.writer.Close(); err != nil {
		return fmt.Errorf("failed to close block writer: %w", err)
	}

	return nil
}

// BlockReader allows reading protobuf blocks from a block file.
type BlockReader struct {
	mux    sync.Mutex // Protects the reader and closed flag.
	closed bool
	reader io.ReaderAt
	buffer *bytes.Buffer
}

// NewBlockReader is the constructor for BlockReader.
func NewBlockReader(reader io.ReaderAt) (*BlockReader, error) {
	if reader == nil {
		return nil, errors.New("expected non-nil reader")
	}
	br := &BlockReader{mux: sync.Mutex{}, reader: reader, closed: false}
	// Call Close when the object is garbage collected.
	runtime.SetFinalizer(br, func(br *BlockReader) { _ = br.Close() })
	return br, nil
}

// ReadBlock reads a proto.Message block from the given offset.
func (br *BlockReader) ReadBlock(offset int64, msg proto.Message) (int64 /*nextOffset*/, error) {
	br.mux.Lock()
	defer br.mux.Unlock()

	if br.closed {
		return 0, errors.New("block reader is closed")
	}

	// Read the block size (8 bytes, little-endian).
	sizeBuf := make([]byte, 8)
	if _, err := br.reader.ReadAt(sizeBuf, int64(offset)); err != nil {
		return 0, fmt.Errorf("failed to read block size: %w", err)
	}

	// Read the block data.
	blockSize := int64(binary.LittleEndian.Uint64(sizeBuf))
	sectionReader := io.NewSectionReader(br.reader, int64(offset+8), blockSize)
	blockBuffer := bufferPool.Get().(*bytes.Buffer)
	defer func() {
		blockBuffer.Reset()
		bufferPool.Put(blockBuffer)
	}()
	readBytes, err := blockBuffer.ReadFrom(sectionReader)
	if err != nil {
		return 0, fmt.Errorf("failed to read block data: %w", err)
	}
	if readBytes != blockSize {
		utils.RaiseInvariant("block", "incomplete_read", "Read an incomplete block.",
			"expected", blockSize, "actual", readBytes)
		return 0, fmt.Errorf("incomplete block read: expected %d bytes, got %d bytes", blockSize, readBytes)
	}

	// Unmarshal data block.
	if err := proto.Unmarshal(blockBuffer.Bytes(), msg); err != nil {
		return 0, fmt.Errorf("failed to unmarshal block data: %w", err)
	}

	return offset + 8 + readBytes /*nextOffset*/, nil
}

// Close releases resources used by the BlockReader.
func (br *BlockReader) Close() error {
	br.mux.Lock()
	defer func() {
		// Give back the buffer to the pool.
		if br.buffer != nil {
			br.buffer.Reset()
			bufferPool.Put(br.buffer)
			br.buffer = nil
		}
		br.closed = true
		br.mux.Unlock()
	}()

	if br.closed {
		return errors.New("block reader is already closed")
	}

	return nil
}
