package storage

import (
	"errors"
	"io"
	"os"
	"path"
	"testing"

	kiwipb "github.com/nobletooth/kiwi/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

// TestBlockStorage is a smoke test for the whole block storage system.
func TestBlockStorage(t *testing.T) {
	filePath := path.Join(t.TempDir(), "test.block")

	// The test records include variable length data.
	expected := []*kiwipb.TestRecord{
		{Id: 12, Name: "test_record_12"},
		{Id: 1234, Name: "test_record_1234"},
		{Id: 567, Name: "test_record_567"},
	}
	{ // writeBytes the records.
		tmpFile, err := os.Create(filePath)
		assert.NoError(t, err)
		writer, err := NewBlockWriter(tmpFile)
		assert.NoError(t, err)
		for _, record := range expected {
			assert.NoError(t, writer.WriteBlock(record))
		}
		assert.NoError(t, writer.Close())
	}
	got := make([]*kiwipb.TestRecord, 0, len(expected))
	{ // Read the records back.
		tmpFile, err := os.Open(filePath)
		assert.NoError(t, err)
		reader, err := NewBlockReader(tmpFile)
		assert.NoError(t, err)
		offset, messageIdx := int64(0), int64(0)
		for {
			msg := &kiwipb.TestRecord{}
			nextOffset, err := reader.ReadBlock(offset, msg)
			if errors.Is(err, io.EOF) {
				assert.Zero(t, nextOffset)
				break
			} else {
				require.NoError(t, err)
			}
			// Each block should be 8 bytes (length prefix) + the size of the proto message.
			assert.Equal(t, int64(8+proto.Size(expected[messageIdx])), nextOffset-offset)
			got = append(got, msg)
			offset = nextOffset
			messageIdx++
		}
	}
	require.Equal(t, len(expected), len(got), "Expected both slices to have the same length")
	assert.EqualExportedValues(t, expected, got)
}
