/*
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
*/

package historyserver

import (
	"bufio"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"sort"
	"sync"
)

const DictionaryV1 = "YK_DICT_V1"

type Dictionary struct {
	entries map[string]uint32
	keys    []string
	sorted  bool
	lock    sync.RWMutex
}

// NewDictionary creates a new dictionary
func NewDictionary() *Dictionary {
	return &Dictionary{
		entries: make(map[string]uint32),
		keys:    make([]string, 0),
		sorted:  true,
	}
}

// LookupID returns the ID associated with the given symbol
func (d *Dictionary) LookupID(symbol string) (uint32, bool) {
	d.lock.RLock()
	defer d.lock.RUnlock()
	id, ok := d.entries[symbol]
	return id, ok
}

// LookupSymbol returns the symbol associated with the given ID
func (d *Dictionary) LookupSymbol(id uint32) (string, bool) {
	d.lock.RLock()
	defer d.lock.RUnlock()
	if int(id) >= len(d.keys) {
		return "", false
	}
	return d.keys[int(id)], true
}

// AddSymbol adds a new entry to this dictionary
func (d *Dictionary) AddSymbol(key string) uint32 {
	d.lock.Lock()
	defer d.lock.Unlock()
	id, ok := d.entries[key]
	if !ok {
		id = uint32(len(d.keys))
		d.entries[key] = id
		d.keys = append(d.keys, key)
		d.sorted = false
	}
	return id
}

// Sort returns a new dictionary which is a sorted (and possibly renumbered) version of this one
func (d *Dictionary) Sort() *Dictionary {
	d.lock.Lock()
	defer d.lock.Unlock()
	result := &Dictionary{
		entries: make(map[string]uint32),
		keys:    make([]string, len(d.keys)),
		sorted:  true,
	}
	copy(result.keys, d.keys)
	sort.Strings(result.keys)
	for idx, key := range result.keys {
		result.entries[key] = uint32(idx)
	}
	return result
}

// Save writes this dictionary (must be sorted) to a file
func (d *Dictionary) Save(filename string) error {
	d.lock.RLock()
	defer d.lock.RUnlock()
	if !d.sorted {
		return errors.New("dictionary is not sorted")
	}
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()
	bfile := bufio.NewWriter(file)
	if err := bfile.WriteByte(byte(len(DictionaryV1))); err != nil {
		return err
	}
	if _, err := bfile.WriteString(DictionaryV1); err != nil {
		return err
	}
	for _, key := range d.keys {
		if err := binary.Write(bfile, binary.BigEndian, uint32(len(key))); err != nil {
			return err
		}
		if _, err := bfile.WriteString(key); err != nil {
			return err
		}
	}
	if err := bfile.Flush(); err != nil {
		return err
	}
	return nil
}

// NewDictionaryFromFile loads a dictionary object from a file
func NewDictionaryFromFile(filename string) (*Dictionary, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	bfile := bufio.NewReader(file)
	headerLen := make([]byte, 1)
	bytesRead, err := io.ReadFull(bfile, headerLen)
	if err != nil {
		return nil, err
	}
	if bytesRead != 1 {
		return nil, errors.New("invalid dictionary header length")
	}
	version := make([]byte, headerLen[0])
	bytesRead, err = io.ReadFull(bfile, version)
	if err != nil {
		return nil, err
	}
	if bytesRead != len(version) {
		return nil, errors.New("invalid dictionary header")
	}
	switch string(version) {
	case DictionaryV1:
		break
	default:
		return nil, errors.New("unknown dictionary version")
	}

	d := &Dictionary{
		entries: make(map[string]uint32),
		keys:    make([]string, 0),
		sorted:  true,
	}
	lenBuf := make([]byte, 4)
	for {
		bytesRead, err = io.ReadFull(bfile, lenBuf)
		if err != nil && errors.Is(err, io.EOF) {
			break
		}
		if bytesRead != 4 {
			return nil, errors.New("corrupt dictionary")
		}
		bytesToRead := binary.BigEndian.Uint32(lenBuf)
		keyBuf := make([]byte, bytesToRead)
		bytesRead, err = io.ReadFull(bfile, keyBuf)
		if err != nil {
			return nil, err
		}
		if bytesRead != len(keyBuf) {
			return nil, errors.New("corrupt dictionary")
		}
		d.keys = append(d.keys, string(keyBuf))
		d.entries[string(keyBuf)] = uint32(len(d.keys))
	}
	return d, nil
}
