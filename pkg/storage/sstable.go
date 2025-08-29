package storage

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"sync"

	"github.com/nobletooth/kiwi/pkg/utils"
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

// BlockCache is an in-memory cache that reduces disk reads for frequently accessed data blocks.
// TODO: Make this an actual cache.
type BlockCache struct {
	mux  sync.Mutex
	data map[dataBlockKey]*kiwipb.DataBlock
}

// getSharedCache returns the singleton shared block cache instance.
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

// SSTable represents a single immutable sorted string table stored on disk.
// TODO Add Bloom filter index.
type SSTable struct {
	mux    sync.Mutex // Protects against concurrent files.
	closed bool
	table  int64 // The table id this SSTable belongs to, e.g. 123 in /path/to/data/123/456.sst

	file        *os.File // A readonly file used by blockReader.
	blockReader *BlockReader
	header      *kiwipb.PartHeader // Eagerly loaded into memory.
	skipIndex   *kiwipb.SkipIndex  // Eagerly loaded into memory.
	sharedCache *BlockCache        // Allows access to data blocks.
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

// getFromDataBlocks scans through the cached and on-disk data blocks to find the value for the given key.
func (s *SSTable) getFromDataBlocks(key []byte) ([]byte, error) {
	// Since the skip index is sorted by key prefixes, we can use binary search to find the right data block.
	// We do a prefix search to find the first block whose prefix is greater than the key. Since prefixes may
	// have collisions, we need to scan backwards to get the complete matching range.
	// For instance, when searching for "abc", we may end up on the "ab" prefix in the index, so we need to go back
	// to find the "a" or the "" (empty) prefix as well. We may even see multiple "ab" prefixes.
	blockPrefixes := s.skipIndex.GetPrefix()
	endIndex, _ := slices.BinarySearchFunc(blockPrefixes, key, bytes.Compare)
	startIndex := endIndex
	for startIndex > 0 && bytes.HasPrefix(blockPrefixes[startIndex-1], key) {
		startIndex--
	}

	// Now that we have the proper block range, we need to scan each block for the key.
	sstableId := s.header.GetId() // Cache to avoid expensive heap calls.
	blockOffsets := s.skipIndex.GetBlockOffsets()
	for i := startIndex; i < endIndex; i++ {
		// Read the data block.
		blockOffset := blockOffsets[i]
		var dataBlock *kiwipb.DataBlock
		if cachedBlock, exists := s.sharedCache.Get(s.table, sstableId, blockOffset); exists {
			// Read from in-memory data block cache.
			dataBlock = cachedBlock
		} else {
			// Read from disk part and populate the cache.
			dataBlock = &kiwipb.DataBlock{}
			if _, err := s.blockReader.ReadBlock(blockOffset, dataBlock); err != nil {
				return nil, fmt.Errorf("failed to read data block at offset %d: %w", blockOffset, err)
			}
			s.sharedCache.Set(s.table, sstableId, blockOffset, dataBlock)
		}

		// Now that we have the data block, we can scan it for the key. Note that the keys in the data block
		// are stripped of their mutual prefix aforementioned in the skip index.
		keyWithoutPrefix := bytes.TrimPrefix(key, blockPrefixes[i])
		if keyIndex, found := slices.BinarySearchFunc(dataBlock.GetKeys(), keyWithoutPrefix, bytes.Compare); found {
			return dataBlock.GetValues()[keyIndex], nil
		} else { // Key not found in this block, continue to next block.
			continue
		}
	}

	return nil, ErrKeyNotFound
}

// GetPrevTablePath returns the file path of the previous SSTable in the chain, if any.
func (s *SSTable) GetPrevTablePath() (string /*filePath*/, bool /*hasPrevious*/) {
	s.mux.Lock()
	defer s.mux.Unlock()

	prevPartId := s.header.GetPrevPart()
	if prevPartId == 0 {
		return "", false
	}

	prevFilePath := filepath.Join(filepath.Dir(s.file.Name()), fmt.Sprintf("%d.sst", prevPartId))
	if _, err := os.Stat(prevFilePath); errors.Is(err, os.ErrNotExist) {
		utils.RaiseInvariant("sstable", "non_existent_prev_table",
			"The previous sstable file does not exist.", "previousFile", prevFilePath)
		return "", false
	}

	return prevFilePath, true
}

func (s *SSTable) Get(key []byte) ([]byte, error) {
	s.mux.Lock()
	defer s.mux.Unlock()
	// When the SSTable is closed, we cannot read from it anymore.
	if s.closed {
		return nil, errors.New("sstable is closed")
	}
	// First, check if the key is within the min/max range of the SSTable.
	if bytes.Compare(key, s.skipIndex.GetFirstKey()) < 0 || bytes.Compare(key, s.skipIndex.GetLastKey()) > 0 {
		return nil, ErrKeyNotFound
	}
	// The key is within the range, so we need to scan the data blocks.
	return s.getFromDataBlocks(key)
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
