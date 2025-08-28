package storage

import (
	"bytes"
	"fmt"
	"io"
	"runtime"
	"sync"
)

const defaultBufferSize = 4096

var bufferPool = sync.Pool{New: func() any { return bytes.NewBuffer(make([]byte, 0, defaultBufferSize)) }}

type Block []byte

type BlockWriter struct { // Implements io.Writer.
	mux           sync.Mutex
	currentOffset int
	writer        io.WriteCloser
	buffer        *bytes.Buffer
}

var _ io.WriteCloser = (*BlockWriter)(nil)

// NewBlockWriter is the constructor for BlockWriter.
func NewBlockWriter(writer io.WriteCloser) *BlockWriter {
	bw := &BlockWriter{mux: sync.Mutex{}, writer: writer, currentOffset: 0}
	// Call Close when the object is garbage collected.
	runtime.SetFinalizer(bw, func(bw *BlockWriter) { _ = bw.Close() })
	return bw
}

func (bw *BlockWriter) Write(p []byte) (flushed int, err error) {
	bw.mux.Lock()
	defer bw.mux.Unlock()

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
			bw.buffer.Write(p[flushed:]) // Write all remaining bytes.
			flushed += toFlush
			toFlush = 0
		}
	}

	return flushed, nil
}

func (bw *BlockWriter) Close() error {
	defer func() { // Give back the buffer to the pool.
		bw.buffer.Reset()
		bufferPool.Put(bw.buffer)
		bw.buffer = nil
	}()

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
