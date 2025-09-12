// SSTables are immutable on-disk files that store sorted key-value pairs. A Kiwi table is separated into a
// chain of SSTables, where each SSTable contains a subset of the table's data and is composed of multiple blocks,
// including a header block, a skip index block, an optional bloom filter block, and multiple data blocks.
// The header and skip index blocks are eagerly loaded into memory when the SSTable is opened.
// The data blocks are lazily loaded on demand when a key is requested. To reduce disk reads, frequently accessed
// data blocks are cached in memory using a shared block cache.

package storage

import (
	"bytes"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"sync"

	"github.com/bits-and-blooms/bloom/v3"
	"github.com/nobletooth/kiwi/pkg/utils"
	kiwipb "github.com/nobletooth/kiwi/proto"
)

var (
	tmpFolder = flag.String("temp_folder", os.TempDir(), "Temporary folder for SSTable writes.")

	bfIndexFalsePositiveRate = flag.Float64("bloom_filter_false_positive_rate", 0.01,
		"A ratio in [0.0, 1.0] that indicates the desired false positive rate for the bloom filter index of"+
			" each datablock.")
	bfIndexMinKeys = flag.Uint("bloom_filter_min_keys", 5,
		"The minimum number of keys in a data block to create a bloom filter index for it.")
)

// getBloomFalsePositiveRate returns a clipped false positive rate for the bloom filter index to avoid panics.
func getBloomFalsePositiveRate() float64 {
	if *bfIndexFalsePositiveRate > 0.0 && *bfIndexFalsePositiveRate < 1.0 {
		return *bfIndexFalsePositiveRate
	}
	utils.RaiseInvariant("sstable", "invalid_bloom_filter_rate",
		"Bloom filter false positive rate must be in (0.0, 1.0). Using default value 0.01.",
		"providedRate", *bfIndexFalsePositiveRate)
	return 0.01
}

// writeSSTable writes the given key-value pairs to an SSTable file at the specified path.
func writeSSTable(prevId, nextId int64, path string, pairs []BytePair) error {
	if len(pairs) == 0 {
		return errors.New("expected a non-empty list of pairs")
	}

	// Compress the pairs into data blocks and their corresponding prefixes.
	prefixes, dataBlocks := compressDataBlocks(pairs)
	if len(prefixes) != len(dataBlocks) {
		utils.RaiseInvariant("chain", "datablock_prefix_size_mismatch",
			"Expected the same number of prefixes and data blocks.",
			"prefixes", len(prefixes), "dataBlocks", len(dataBlocks))
		return errors.New("expected the same number of prefixes and data blocks")
	}

	// Build header.
	dataBlockOffsets := make([]int64, len(dataBlocks))
	lastBlockOffset := int64(0)
	firstKeys := make([][]byte, len(dataBlocks))
	for i, block := range dataBlocks {
		dataBlockOffsets[i] = lastBlockOffset
		lastBlockOffset += getBlockSize(block)
		firstKeys[i] = slices.Concat(prefixes[i], block.GetKeys()[0])
	}
	lastDBlockIndex := len(dataBlocks) - 1
	lastKeyIndex := len(dataBlocks[lastDBlockIndex].GetKeys()) - 1
	// Optionally create a bloom filter index for this SSTable.
	var bf *kiwipb.PartHeader_BloomFilterIndex
	if len(pairs) >= int(*bfIndexMinKeys) {
		bfIndex := bloom.NewWithEstimates(uint(len(pairs)), getBloomFalsePositiveRate())
		for _, pair := range pairs {
			bfIndex.Add(pair.Key)
		}
		bf = &kiwipb.PartHeader_BloomFilterIndex{
			NumBits:      uint64(bfIndex.Cap()),
			NumHashFuncs: uint64(bfIndex.K()),
			BitArray:     bfIndex.BitSet().Words(),
		}
		slog.Info("Constructed bloom filter for sstable.", "path", path, "numKeys", len(pairs),
			"numBits", bf.NumBits, "numHashFuncs", bf.NumHashFuncs)
	}
	header := &kiwipb.PartHeader{
		Id:       nextId,
		PrevPart: prevId,
		BfIndex:  bf,
		SkipIndex: &kiwipb.PartHeader_SkipIndex{
			Prefixes:     prefixes,
			FirstKeys:    firstKeys,
			LastKey:      slices.Concat(prefixes[lastDBlockIndex], dataBlocks[lastDBlockIndex].GetKeys()[lastKeyIndex]),
			BlockOffsets: dataBlockOffsets,
		},
	}

	// Write blocks into a temporary file first.
	tmpFile, err := os.CreateTemp(*tmpFolder, "sstable_*.tmp")
	if err != nil || tmpFile == nil {
		return fmt.Errorf("failed to create temp file for sstable: %w", err)
	}
	defer func() { _ = os.Remove(tmpFile.Name()) }()
	blockWriter, err := NewBlockWriter(tmpFile)
	if err != nil {
		return fmt.Errorf("failed to create block writer for sstable: %w", err)
	}
	if err := blockWriter.WriteBlock(header); err != nil {
		return fmt.Errorf("failed to write header block for sstable: %w", err)
	}
	for _, dataBlock := range dataBlocks {
		if err := blockWriter.WriteBlock(dataBlock); err != nil {
			return fmt.Errorf("failed to write data block for sstable: %w", err)
		}
	}
	if err := blockWriter.Close(); err != nil { // Flush all data.
		return fmt.Errorf("failed to close block writer for sstable: %w", err)
	}

	// Rename the temporary file to the final path.
	if err := os.MkdirAll(filepath.Dir(path), 0o755 /*perm*/); err != nil {
		return fmt.Errorf("failed to create dirs for sstable path '%s': %w", path, err)
	}
	if err := os.Rename(tmpFile.Name(), path); err != nil {
		return fmt.Errorf("failed to rename temp file '%s' to final path '%s': %w", tmpFile.Name(), path, err)
	}

	return nil
}

// SSTable represents a single immutable sorted string table stored on disk.
type SSTable struct {
	mux    sync.Mutex // Protects against concurrent files.
	closed bool
	table  int64 // The table id this SSTable belongs to, e.g. 123 in /path/to/data/123/456.sst

	blockReader     *BlockReader       // Reads header and data blocks.
	file            *os.File           // A readonly file used by blockReader.
	dataBlockOffset int64              // The byte offset where data blocks start in the file.
	header          *kiwipb.PartHeader // Eagerly loaded into memory.
	bloomFilter     *bloom.BloomFilter // Optional bloom filter for the entire SSTable key space.
	sharedCache     *BlockCache        // Allows access to data blocks.
}

// NewSSTable is the constructor for SSTable.
func NewSSTable(filePath string) (*SSTable, error) {
	slog.Debug("Opening SSTable file.", "filePath", filePath)
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
	headerSize := getBlockSize(partHeader)

	// Instantiate the optional bloom filter.
	var bf *bloom.BloomFilter
	if bfIndex := partHeader.GetBfIndex(); bfIndex != nil {
		slog.Debug("Loading bloom filter for sstable", "table", table, "part", partHeader.GetId())
		bf = bloom.FromWithM(bfIndex.GetBitArray(), uint(bfIndex.GetNumBits()), uint(bfIndex.GetNumHashFuncs()))
		if bf == nil {
			utils.RaiseInvariant("sstable", "bloom_filter_corruption", "Failed to load bloom filter from sstable.",
				"table", table, "part", partHeader.GetId())
		} else {
			slog.Debug("Loaded bloom filter for sstable", "table", table, "part", partHeader.GetId(),
				"numBits", bfIndex.GetNumBits(), "numHashFuncs", bfIndex.GetNumHashFuncs())
		}
		// Free memory used by the protobuf bloom filter index, as it's no longer needed.
		partHeader.BfIndex = nil
	}

	ssTable := &SSTable{
		blockReader: bw, file: file, table: table, bloomFilter: bf,
		header: partHeader, sharedCache: getSharedCache(), closed: false,
		// The data blocks start right after the header block.
		dataBlockOffset: headerSize,
	}
	// Call Close when the object is garbage collected.
	runtime.SetFinalizer(ssTable, func(ssTable *SSTable) { _ = ssTable.Close() })
	return ssTable, nil
}

// getFromDataBlocks scans through the cached and on-disk data blocks to find the value for the given key.
func (s *SSTable) getFromDataBlocks(key []byte) ([]byte, error) {
	// Since the skip index is sorted by key prefixes, we can use binary search to find the right data block.
	// blockIndex is the first block whose first key is less than the target key. We don't care if we find an
	// exact match, but the found block needs to be fully scanned.
	firstKeys := s.header.GetSkipIndex().GetFirstKeys()
	blockIndex, found := slices.BinarySearchFunc(firstKeys, key, bytes.Compare)
	if !found { // When not found, BinarySearchFunc returns the index where the key would be inserted.
		if blockIndex == 0 {
			// Key is smaller than the first key in the skip index, so it cannot be in this SSTable.
			return nil, ErrKeyNotFound
		} else {
			// This is not the first block, so we need to check the previous block.
			// E.g. if the first keys are [a, d, g] and we're looking for 'e', we need to check the block
			// starting with 'd'.
			blockIndex--
		}
	}

	// Now that we have the proper block range, we need to scan each block for the key.
	sstableId := s.header.GetId() // Cache to avoid expensive heap calls.
	blockOffsets := s.header.GetSkipIndex().GetBlockOffsets()
	blockPrefixes := s.header.GetSkipIndex().GetPrefixes()
	// Read the data block.
	blockOffset := blockOffsets[blockIndex] + s.dataBlockOffset
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
	keyWithoutPrefix := bytes.TrimPrefix(key, blockPrefixes[blockIndex])
	if keyIndex, found := slices.BinarySearchFunc(dataBlock.GetKeys(), keyWithoutPrefix, bytes.Compare); found {
		return dataBlock.GetValues()[keyIndex], nil
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

	// Check if the key is within the min/max range of the SSTable.
	skipIndex := s.header.GetSkipIndex()
	if bytes.Compare(key, skipIndex.GetFirstKeys()[0]) < 0 || bytes.Compare(key, skipIndex.GetLastKey()) > 0 {
		return nil, ErrKeyNotFound
	}

	// The bloom filter can show when the key is definitely not in this SSTable.
	// On false positives, we still need to scan the data blocks.
	if s.bloomFilter != nil && !s.bloomFilter.Test(key) {
		return nil, ErrKeyNotFound
	}

	return s.getFromDataBlocks(key)
}

func (s *SSTable) Table() int64 {
	return s.table
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
