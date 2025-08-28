package storage

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"sync"

	kiwipb "github.com/nobletooth/kiwi/proto"
)

var (
	initCacheOnce sync.Once
	sharedCache   *BlockCache
)

type dataBlockKey struct {
	table     int64
	ssTableId int64
	offset    int64
}

// TODO: Make this an actual cache.

type BlockCache struct {
	mux  sync.Mutex
	data map[dataBlockKey]*kiwipb.DataBlock
}

// getSharedCache is the constructor for BlockCache.
func getSharedCache() *BlockCache {
	initCacheOnce.Do(func() {
		sharedCache = &BlockCache{mux: sync.Mutex{}, data: make(map[dataBlockKey]*kiwipb.DataBlock)}
	})
	return sharedCache
}

func (p *BlockCache) Get(table, ssTableId, offset int64) (*kiwipb.DataBlock, bool) {
	p.mux.Lock()
	defer p.mux.Unlock()
	val, exists := p.data[dataBlockKey{table: table, ssTableId: ssTableId, offset: offset}]
	return val, exists
}

func (p *BlockCache) Set(table, ssTableId, offset int64, block *kiwipb.DataBlock) {
	p.mux.Lock()
	defer p.mux.Unlock()
	p.data[dataBlockKey{table: table, ssTableId: ssTableId, offset: offset}] = block
}

type SSTable struct {
	mux         sync.Mutex // Protects against concurrent files.
	closed      bool
	file        *os.File // A readonly file accessed by blockReader.
	blockReader *BlockReader

	table     int64
	header    *kiwipb.PartHeader
	skipIndex *kiwipb.SkipIndex
	// TODO Add Bloom filter index.
	sharedCache *BlockCache
}

// NewSSTable is the constructor for SSTable.
func NewSSTable(filePath string) (*SSTable, error) {
	// Each SSTable is a single file stored in a directory named after its table id.
	// The file name is the sstable id, e.g. /path/to/data/123/456.sst
	dir := filepath.Base(filepath.Dir(filePath))
	table, err := strconv.ParseInt(dir, 10 /*base*/, 64 /*bitSize*/)
	if err != nil {
		return nil, fmt.Errorf("failed to parse sstable dir id %q: %w", dir, err)
	}
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open sstable file: %w", err)
	}

	// The header blocks of the SSTable are always eagerly read into memory, as they're small and always needed.
	// The data blocks on the other hand, are lazily read on demand.
	bw, err := NewBlockReader(file)
	if err != nil {
		return nil, fmt.Errorf("failed to create sstable: %w", err)
	}
	partHeader := &kiwipb.PartHeader{}
	if _, err := bw.ReadBlock(0 /*offset*/, partHeader); err != nil {
		return nil, fmt.Errorf("failed to read sstable part header: %w", err)
	}
	skipIndex := &kiwipb.SkipIndex{}
	if _, err := bw.ReadBlock(partHeader.GetSkipIndexOffset(), skipIndex); err != nil {
		return nil, fmt.Errorf("failed to read sstable skip index: %w", err)
	}

	ssTable := &SSTable{
		blockReader: bw, file: file, table: table,
		header: partHeader, skipIndex: skipIndex,
		sharedCache: getSharedCache(),
		closed:      false,
	}
	// Call Close when the object is garbage collected.
	runtime.SetFinalizer(ssTable, func(ssTable *SSTable) { _ = ssTable.Close() })
	return ssTable, nil
}

func (s *SSTable) Get(key string) (string, error) {
	//TODO implement me
	panic("implement me")
}

func (s *SSTable) Close() error {
	s.mux.Lock()
	defer s.mux.Unlock()

	if s.closed {
		return errors.New("sstable already closed")
	}

	readerCloseErr := s.blockReader.Close()
	fileCloseErr := s.file.Close()
	if err := errors.Join(readerCloseErr, fileCloseErr); err != nil {
		return fmt.Errorf("failed to close sstable: %w", err)
	}
	s.closed = true

	return nil
}
