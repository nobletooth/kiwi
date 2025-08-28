package main

import (
	"bytes"
	"cmp"
	"errors"
	"io"
	"io/fs"
	"os"
	"slices"
	"strconv"
	"time"
)

const (
	kSize = 8
	vSize = 2
)

type MemTable struct {
	data map[string]string
}

func newMemTable() *MemTable {
	return &MemTable{data: make(map[string]string)}
}

func (m *MemTable) Get(key string) (string, error) {
	if val, exists := m.data[key]; exists {
		return val, nil
	}
	return "", errors.New("key not found")
}

func (m *MemTable) Set(key, val string) {
	m.data[key] = val
}

type SSTable struct {
	file *os.File
}

func newSSTable(memTable *MemTable) (*SSTable, error) {
	path := strconv.Itoa(time.Now().Nanosecond()) + ".sst"
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	kvs := make([][2]string, len(memTable.data))
	for key, val := range memTable.data {
		kvs = append(kvs, [2]string{key, val})
	}
	slices.SortFunc(kvs, func(a, b [2]string) int {
		return cmp.Compare(a[0], b[0])
	})

	buffer := bytes.NewBuffer(nil)
	for _, pair := range kvs {
		buffer.WriteString(pair[0] + pair[1])
	}

	toWrite := buffer.Bytes()
	if writtenBytes, err := file.Write(toWrite); err != nil || writtenBytes != len(toWrite) {
		return nil, err
	}

	return &SSTable{file: file}, nil
}

func (s *SSTable) Get(key string) (string, error) {
	if s.file == nil {
		return "", errors.New("file not found")
	}

	content, err := io.ReadAll(s.file)
	if err != nil {
		return "", err
	}

	kvs := make([][2]string, len(content)/(kSize+vSize))
	for i := 0; i < len(content); i += kSize + vSize {
		k := string(content[i : i+kSize])
		v := string(content[i+kSize : i+kSize+vSize])
		kvs[i/(kSize+vSize)] = [2]string{k, v}
	}

	idx, found := slices.BinarySearchFunc(kvs, [2]string{key, ""}, func(a, b [2]string) int {
		return cmp.Compare(a[0], b[0])
	})
	if found {
		return kvs[idx][1], nil
	}

	return "", errors.New("key not found")
}

type LSMTree struct {
	ssTables []*SSTable
	memTable *MemTable
}

func newLSMTree() *LSMTree {
	ssTables := make([]*SSTable, 0)
	err := fs.WalkDir(os.DirFS("."), ".", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() && len(path) > 4 && path[len(path)-4:] == ".sst" {
			// load sst file
			file, err := os.OpenFile(path, os.O_RDWR, 0644)
			if err != nil {
				return err
			}
			ssTables = append(ssTables, &SSTable{file: file})
		}
		return nil
	})
	if err != nil {
		panic(err)
	}

	return &LSMTree{ssTables: ssTables, memTable: newMemTable()}
}

func (l *LSMTree) Get(key string) (string, error) {
	for i := len(l.ssTables) - 1; i >= 0; i-- {
		if val, err := l.ssTables[i].Get(key); err == nil {
			return val, nil
		}
	}
	return "", errors.New("key not found")
}

func (l *LSMTree) Set(key, val string) {
	l.memTable.Set(key, val)
	if len(l.memTable.data) > 1000 {
		newSSTable, err := newSSTable(l.memTable)
		if err != nil {
			panic(err)
		}
		l.ssTables = append(l.ssTables, newSSTable)
		l.memTable = newMemTable()
	}
}

func (l *LSMTree) Flush() {
	_, err := newSSTable(l.memTable)
	if err != nil {
		panic(err)
	}
}
