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
	"time"
)

const WriteAheadLogV1 = "YK_WAL_V1"

var WriteAheadLogV1Len = []byte{byte(len(WriteAheadLogV1))}

type WriteAheadLogEntry struct {
	ApplicationID string
	TaskID        string
	TaskName      string
	UserName      string
	NodeName      string
	InstanceType  string
	ResourceID    string
	EventTime     time.Time
	DurationSecs  uint32
	Quantity      uint64
}

type WriteAheadLogWriter struct {
	file   *os.File
	writer *bufio.Writer
	size   uint64
}

type WriteAheadLogReader struct {
	file   *os.File
	reader *bufio.Reader
	size   uint64
}

// NewWriteAheadLogWriter creates a write-head log with the given filename
func NewWriteAheadLogWriter(fileName string) (*WriteAheadLogWriter, error) {
	file, err := os.Create(fileName)
	if err != nil {
		return nil, err
	}
	writer := bufio.NewWriter(file)
	if _, err := writer.Write(WriteAheadLogV1Len); err != nil {
		file.Close()
		return nil, err
	}
	if _, err := writer.WriteString(WriteAheadLogV1); err != nil {
		file.Close()
		return nil, err
	}
	return &WriteAheadLogWriter{
		file:   file,
		writer: writer,
		size:   uint64(len(WriteAheadLogV1) + 1),
	}, nil
}

// GetSize returns the number of bytes written to the log
func (w *WriteAheadLogWriter) GetSize() uint64 {
	return w.size
}

// Close flushes and writes the final EOF marker to the log
func (w *WriteAheadLogWriter) Close() error {
	if w == nil {
		return nil
	}
	// write end of file tag
	if err := w.writer.WriteByte(4); err != nil {
		_ = w.writer.Flush()
		_ = w.file.Close()
		return err
	}
	w.size++
	// flush buffer
	if err := w.writer.Flush(); err != nil {
		_ = w.file.Close()
		return err
	}
	// close file
	if err := w.file.Close(); err != nil {
		return err
	}
	return nil
}

// WriteEntry serializes a WAL entry to the log
func (w *WriteAheadLogWriter) WriteEntry(entry *WriteAheadLogEntry) error {
	// write start of record tag
	if err := w.writer.WriteByte(1); err != nil {
		return err
	}
	w.size++
	// write string fields
	if err := w.writeStrings(entry.ApplicationID, entry.TaskID, entry.TaskName, entry.UserName, entry.NodeName, entry.InstanceType, entry.ResourceID); err != nil {
		return err
	}
	// write event time
	if err := binary.Write(w.writer, binary.BigEndian, entry.EventTime.Unix()); err != nil {
		return err
	}
	w.size += 8
	// write duration
	if err := binary.Write(w.writer, binary.BigEndian, entry.DurationSecs); err != nil {
		return err
	}
	w.size += 4
	// write quantity
	if err := binary.Write(w.writer, binary.BigEndian, entry.Quantity); err != nil {
		return err
	}
	w.size += 8
	// write end of record tag
	if err := w.writer.WriteByte(2); err != nil {
		return err
	}
	w.size++
	// flush buffer
	if err := w.writer.Flush(); err != nil {
		return err
	}
	return nil
}

func (w *WriteAheadLogWriter) writeStrings(values ...string) error {
	for _, value := range values {
		if err := binary.Write(w.writer, binary.BigEndian, uint32(len(value))); err != nil {
			return err
		}
		if _, err := w.writer.WriteString(value); err != nil {
			return err
		}
		w.size += uint64(len(value) + 4)
	}
	return nil
}

// NewWriteAheadLogReader creates a log reader pointed at the given filename
func NewWriteAheadLogReader(fileName string) (*WriteAheadLogReader, error) {
	file, err := os.Open(fileName)
	if err != nil {
		return nil, err
	}
	reader := bufio.NewReader(file)
	headerLen := make([]byte, 1)
	bytesRead, err := io.ReadFull(reader, headerLen)
	if err != nil {
		return nil, err
	}
	if bytesRead != 1 {
		return nil, errors.New("invalid wal header length")
	}
	version := make([]byte, headerLen[0])
	bytesRead, err = io.ReadFull(reader, version)
	if err != nil {
		return nil, err
	}
	if bytesRead != len(version) {
		return nil, errors.New("invalid wal header")
	}
	switch string(version) {
	case WriteAheadLogV1:
		break
	default:
		return nil, errors.New("unknown wal version")
	}

	return &WriteAheadLogReader{
		file:   file,
		reader: reader,
		size:   uint64(len(version) + 1),
	}, nil
}

// ReadEntry returns the next WAL entry in the log. If log entry was read successfully, result
// will be (*entry, true, nil). If EOF is reached, result will be (nil, false, nil), and if
// an unexpected error occurs (i.e. corrupt data), result will be (nil, false, err).
func (r *WriteAheadLogReader) ReadEntry() (*WriteAheadLogEntry, bool, error) {
	// read start-of-record marker
	startMarker, err := r.reader.ReadByte()
	if err != nil {
		// short read; this probably means the buffer wasn't flushed, but previous entry is still usable
		return nil, false, nil
	}
	r.size++
	if startMarker == byte(4) {
		// EOF; file was properly closed
		return nil, false, nil
	}
	if startMarker != byte(1) { // start of record not found
		return nil, false, errors.New("corrupt wal entry")
	}
	// read string values
	strings, err := r.readStrings(7)
	if err != nil {
		return nil, false, err
	}
	// read timestamp
	eventTimeBuf := make([]byte, 8)
	bytesRead, err := io.ReadFull(r.reader, eventTimeBuf)
	if err != nil {
		return nil, false, err
	}
	r.size += uint64(bytesRead)
	if bytesRead != 8 {
		return nil, false, errors.New("corrupt wal entry")
	}
	eventTime := time.Unix(int64(binary.BigEndian.Uint64(eventTimeBuf)), 0)
	// read duration seconds
	durationSecsBuf := make([]byte, 4)
	bytesRead, err = io.ReadFull(r.reader, durationSecsBuf)
	if err != nil {
		return nil, false, err
	}
	r.size += uint64(bytesRead)
	if bytesRead != 4 {
		return nil, false, errors.New("corrupt wal entry")
	}
	durationSecs := binary.BigEndian.Uint32(durationSecsBuf)
	// read quantity
	quantityBuf := make([]byte, 8)
	bytesRead, err = io.ReadFull(r.reader, quantityBuf)
	if err != nil {
		return nil, false, err
	}
	r.size += uint64(bytesRead)
	if bytesRead != 8 {
		return nil, false, errors.New("corrupt wal entry")
	}
	quantity := binary.BigEndian.Uint64(quantityBuf)
	// read end-of-record marker
	endMarker, err := r.reader.ReadByte()
	if err != nil {
		return nil, false, err
	}
	r.size++
	if endMarker != byte(2) { // end of record not found
		return nil, false, errors.New("corrupt wal entry")
	}
	return &WriteAheadLogEntry{
		ApplicationID: strings[0],
		TaskID:        strings[1],
		TaskName:      strings[2],
		UserName:      strings[3],
		NodeName:      strings[4],
		InstanceType:  strings[5],
		ResourceID:    strings[6],
		EventTime:     eventTime,
		DurationSecs:  durationSecs,
		Quantity:      quantity,
	}, true, nil
}

func (r *WriteAheadLogReader) readStrings(count int) ([]string, error) {
	result := make([]string, 0)

	lenBuf := make([]byte, 4)
	for i := 0; i < count; i++ {
		bytesRead, err := io.ReadFull(r.reader, lenBuf)
		if err != nil {
			return nil, err
		}
		r.size += uint64(bytesRead)
		if bytesRead != 4 {
			return nil, errors.New("corrupt wal entry")
		}
		bytesToRead := binary.BigEndian.Uint32(lenBuf)
		buf := make([]byte, bytesToRead)
		bytesRead, err = io.ReadFull(r.reader, buf)
		if err != nil {
			return nil, err
		}
		r.size += uint64(bytesRead)
		if bytesRead != int(bytesToRead) {
			return nil, errors.New("corrupt wal entry")
		}
		result = append(result, string(buf))
	}
	return result, nil
}

// Close closes the log
func (r *WriteAheadLogReader) Close() error {
	if r == nil {
		return nil
	}
	return r.file.Close()
}

// GetSize returns the number of bytes read from this log
func (r *WriteAheadLogReader) GetSize() uint64 {
	return r.size
}
