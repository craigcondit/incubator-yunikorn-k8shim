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

type DataPoint struct {
	StartTimeSecs uint32
	DurationSecs  uint32
	Quantity      uint64
}

type TimeSeriesKey struct {
	TaskID     uint32
	ResourceID uint32
}

type InMemoryTimeSeries struct {
	data map[TimeSeriesKey][]DataPoint
	lock sync.RWMutex
}

func NewInMemoryTimeSeries() *InMemoryTimeSeries {
	return &InMemoryTimeSeries{
		data: make(map[TimeSeriesKey][]DataPoint),
	}
}

func (t *InMemoryTimeSeries) GetData(taskID uint32, resourceID uint32) ([]DataPoint, bool) {
	key := TimeSeriesKey{TaskID: taskID, ResourceID: resourceID}
	t.lock.RLock()
	defer t.lock.RUnlock()
	data, ok := t.data[key]
	if !ok {
		return nil, false
	}
	result := make([]DataPoint, len(data))
	copy(result, data)
	return result, true
}

func (t *InMemoryTimeSeries) PutData(taskID uint32, resourceID uint32, data ...DataPoint) {
	key := TimeSeriesKey{TaskID: taskID, ResourceID: resourceID}
	t.lock.Lock()
	defer t.lock.Unlock()
	current, ok := t.data[key]
	if !ok {
		current = make([]DataPoint, 0)
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
func (t *InMemoryTimeSeries) ReKey(tidOld, tidNew, ridOld, ridNew *Dictionary) bool {
	result := true
	t.lock.Lock()
	defer t.lock.Unlock()
	data2 := make(map[TimeSeriesKey][]DataPoint)
	for key, value := range t.data {
		key2 := TimeSeriesKey{}
		taskIDStr, ok := tidOld.LookupSymbol(key.TaskID)
		if !ok {
			// TaskID missing from old dictionary
			result = false
			continue
		}
		key2.TaskID, ok = tidNew.LookupID(taskIDStr)
		if !ok {
			// TaskID missing from new dictionary
			result = false
			continue
		}
		resourceIDStr, ok := ridOld.LookupSymbol(key.ResourceID)
		if !ok {
			// ResourceID missing from old dictionary
			result = false
			continue
		}
		key2.ResourceID, ok = ridNew.LookupID(resourceIDStr)
		if !ok {
			// ResourceID missing from new dictionary
			result = false
			continue
		}
		data2[key2] = value
	}
	t.data = data2
	return result
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
	keys := make([]TimeSeriesKey, 0, len(t.data))
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
	idxBuf := make([]byte, 0, 20)  // index entry is 20 bytes
	dataBuf := make([]byte, 0, 16) // data entry is 16 bytes

	for _, k := range keys {
		v := t.data[k]
		idxBuf = idxBuf[:0]
		idxBuf = binary.BigEndian.AppendUint32(idxBuf, k.ResourceID)
		idxBuf = binary.BigEndian.AppendUint32(idxBuf, k.TaskID)
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

	return nil
}
