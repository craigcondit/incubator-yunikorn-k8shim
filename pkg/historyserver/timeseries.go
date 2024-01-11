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
	"os"
	"sort"
	"sync"
)

const IndexV1 = "YK_IDX_V1"
const TimeSeriesDBV1 = "YK_TSDB_V1"

type TSData struct {
	StartTimeSecs uint32
	DurationSecs  uint32
	Quantity      uint64
}

type TSMetadata struct {
	ApplicationID uint32
	UserName      uint32
	TaskName      uint32
	NodeName      uint32
	InstanceType  uint32
}

type TSKey struct {
	TaskID     uint32
	ResourceID uint32
}

type InMemoryTimeSeries struct {
	metadata map[TSKey]TSMetadata
	data     map[TSKey][]TSData
	lock     sync.RWMutex
}

func NewInMemoryTimeSeries() *InMemoryTimeSeries {
	return &InMemoryTimeSeries{
		metadata: make(map[TSKey]TSMetadata),
		data:     make(map[TSKey][]TSData),
	}
}

func (t *InMemoryTimeSeries) GetData(taskID uint32, resourceID uint32) (TSMetadata, []TSData, bool) {
	key := TSKey{TaskID: taskID, ResourceID: resourceID}
	t.lock.RLock()
	defer t.lock.RUnlock()
	meta, ok := t.metadata[key]
	data, ok2 := t.data[key]
	if !ok || !ok2 {
		return TSMetadata{}, nil, false
	}
	meta2 := meta
	data2 := make([]TSData, len(data))
	copy(data2, data)
	return meta2, data2, true
}

func (t *InMemoryTimeSeries) PutData(taskID uint32, resourceID uint32, metadata TSMetadata, data ...TSData) {
	key := TSKey{TaskID: taskID, ResourceID: resourceID}
	t.lock.Lock()
	defer t.lock.Unlock()
	_, ok := t.metadata[key]
	current, ok2 := t.data[key]
	if !ok || !ok2 {
		t.metadata[key] = metadata
		current = make([]TSData, 0)
	}
	// append data to current list
	current = append(current, data...)
	// sort by start time and then duration
	sort.SliceStable(current, func(i, j int) bool {
		left := current[i]
		right := current[j]
		if left.StartTimeSecs < right.StartTimeSecs {
			return true
		}
		if left.StartTimeSecs > right.StartTimeSecs {
			return false
		}
		return left.DurationSecs < right.DurationSecs
	})
	// update
	t.data[key] = current
}

// ReKey generates new keys based on existing data using a set of old and new dictionaries (i.e. unsorted vs. sorted)
// If there are missing entries in the dictionaries, false will be returned, but the series may be incomplete.
func (t *InMemoryTimeSeries) ReKey(mapper *Mapper) bool {
	result := true
	var ok bool

	t.lock.Lock()
	defer t.lock.Unlock()

	// update metadata
	meta2 := make(map[TSKey]TSMetadata)
	for key, value := range t.metadata {
		key2 := TSKey{}
		if key2.TaskID, ok = mapper.MapTaskID(key.TaskID); !ok {
			result = false
			continue
		}
		if key2.ResourceID, ok = mapper.MapResourceID(key.ResourceID); !ok {
			result = false
			continue
		}
		if meta2[key2], ok = t.reKeyMeta(mapper, value); !ok {
			result = false
			continue
		}
	}
	t.metadata = meta2

	// update data
	data2 := make(map[TSKey][]TSData)
	for key, value := range t.data {
		key2 := TSKey{}
		if key2.TaskID, ok = mapper.MapTaskID(key.TaskID); !ok {
			result = false
			continue
		}
		if key2.ResourceID, ok = mapper.MapResourceID(key.ResourceID); !ok {
			result = false
			continue
		}
		data2[key2] = value
	}
	t.data = data2

	return result
}

func (t *InMemoryTimeSeries) reKeyMeta(mapper *Mapper, metadata TSMetadata) (TSMetadata, bool) {
	var ok bool
	result := TSMetadata{}
	if result.ApplicationID, ok = mapper.MapApplicationID(metadata.ApplicationID); !ok {
		return result, false
	}
	if result.UserName, ok = mapper.MapUserName(metadata.UserName); !ok {
		return result, false
	}
	if result.TaskName, ok = mapper.MapTaskName(metadata.TaskName); !ok {
		return result, false
	}
	if result.NodeName, ok = mapper.MapNodeName(metadata.NodeName); !ok {
		return result, false
	}
	if result.InstanceType, ok = mapper.MapNodeName(metadata.InstanceType); !ok {
		return result, false
	}
	return result, true
}

func (t *InMemoryTimeSeries) Save(idxFile string, tsFile string) error {
	t.lock.RLock()
	defer t.lock.RUnlock()

	var indexOffset, dataOffset uint64

	// open index
	index, err := os.Create(idxFile)
	if err != nil {
		return err
	}
	defer index.Close()
	indexWriter := bufio.NewWriter(index)
	if err := indexWriter.WriteByte(byte(len(IndexV1))); err != nil {
		return err
	}
	indexOffset++
	if _, err := indexWriter.WriteString(IndexV1); err != nil {
		return err
	}
	indexOffset += uint64(len(IndexV1))

	// open tsdb
	tsdb, err := os.Create(tsFile)
	if err != nil {
		return err
	}
	defer tsdb.Close()
	tsdbWriter := bufio.NewWriter(tsdb)
	if err := tsdbWriter.WriteByte(byte(len(TimeSeriesDBV1))); err != nil {
		return err
	}
	dataOffset++
	if _, err := tsdbWriter.WriteString(TimeSeriesDBV1); err != nil {
		return err
	}
	dataOffset += uint64(len(TimeSeriesDBV1))

	// get keys and sort
	keys := make([]TSKey, 0, len(t.data))
	for k := range t.data {
		keys = append(keys, k)
	}
	sort.SliceStable(keys, func(i int, j int) bool {
		left := keys[i]
		right := keys[j]
		if left.TaskID < right.TaskID {
			return true
		}
		if left.TaskID > right.TaskID {
			return false
		}
		return left.ResourceID < right.ResourceID
	})
	idxBuf := make([]byte, 0, 40)  // index entry is 40 bytes
	dataBuf := make([]byte, 0, 16) // data entry is 16 bytes

	for _, k := range keys {
		m := t.metadata[k]
		v := t.data[k]
		idxBuf = idxBuf[:0]
		idxBuf = binary.BigEndian.AppendUint32(idxBuf, k.TaskID)
		idxBuf = binary.BigEndian.AppendUint32(idxBuf, k.ResourceID)
		idxBuf = binary.BigEndian.AppendUint32(idxBuf, m.ApplicationID)
		idxBuf = binary.BigEndian.AppendUint32(idxBuf, m.UserName)
		idxBuf = binary.BigEndian.AppendUint32(idxBuf, m.TaskName)
		idxBuf = binary.BigEndian.AppendUint32(idxBuf, m.NodeName)
		idxBuf = binary.BigEndian.AppendUint32(idxBuf, m.InstanceType)
		idxBuf = binary.BigEndian.AppendUint64(idxBuf, dataOffset)
		idxBuf = binary.BigEndian.AppendUint32(idxBuf, uint32(len(v)))
		bytesWritten, err := indexWriter.Write(idxBuf)
		if err != nil {
			return err
		}
		indexOffset += uint64(bytesWritten)
		for _, entry := range v {
			dataBuf = dataBuf[:0]
			dataBuf = binary.BigEndian.AppendUint32(dataBuf, entry.StartTimeSecs)
			dataBuf = binary.BigEndian.AppendUint32(dataBuf, entry.DurationSecs)
			dataBuf = binary.BigEndian.AppendUint64(dataBuf, entry.Quantity)
			bytesWritten, err := tsdbWriter.Write(dataBuf)
			if err != nil {
				return err
			}
			dataOffset += uint64(bytesWritten)
		}
	}

	if err := indexWriter.Flush(); err != nil {
		return err
	}
	if err := tsdbWriter.Flush(); err != nil {
		return err
	}

	return nil
}
