package taskmq

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"time"
	"unsafe"
)

// Codec defines the contract for task serialization and deserialization
type Codec interface {
	Marshal(t *Task) ([]byte, error)
	Unmarshal(data []byte, t *Task) error
}

// JSONCodec implements the Codec interface using standard JSON encoding
type JSONCodec struct{}

// Marshal serializes a Task to JSON bytes
func (JSONCodec) Marshal(t *Task) ([]byte, error) {
	return json.Marshal(t)
}

// Unmarshal deserializes JSON bytes to a Task
func (JSONCodec) Unmarshal(data []byte, t *Task) error {
	return json.Unmarshal(data, t)
}

// BinaryCodec implements a high-performance, schema-free, zero-allocation custom binary format.
// It maps the Task fields directly using BigEndian byte encoding.
type BinaryCodec struct{}

// Marshal serializes a Task to custom binary bytes
func (BinaryCodec) Marshal(t *Task) ([]byte, error) {
	// Calculate size of the byte slice to perform a single allocation
	size := 0
	size += 2 + len(t.ID)
	size += 2 + len(t.Queue)
	size += 2 + len(t.Name)
	size += 4 + len(t.Payload)
	size += 4 // Retry (int32)
	size += 4 // MaxRetry (int32)
	size += 8 // TimeoutMs (int64)
	size += 2 + len(t.UniqueKey)
	size += 8 // UniqueTTLMs (int64)
	size += 4 + len(t.LastError)
	size += 2 + len(t.CronSpec)
	size += 8 // CreatedAt (int64 Unix nano)

	buf := make([]byte, size)
	offset := 0

	writeString16 := func(s string) {
		binary.BigEndian.PutUint16(buf[offset:], uint16(len(s)))
		offset += 2
		copy(buf[offset:], s)
		offset += len(s)
	}

	writeString32 := func(s string) {
		binary.BigEndian.PutUint32(buf[offset:], uint32(len(s)))
		offset += 4
		copy(buf[offset:], s)
		offset += len(s)
	}

	writeBytes32 := func(b []byte) {
		binary.BigEndian.PutUint32(buf[offset:], uint32(len(b)))
		offset += 4
		copy(buf[offset:], b)
		offset += len(b)
	}

	writeString16(t.ID)
	writeString16(t.Queue)
	writeString16(t.Name)
	writeBytes32(t.Payload)

	binary.BigEndian.PutUint32(buf[offset:], uint32(t.Retry))
	offset += 4
	binary.BigEndian.PutUint32(buf[offset:], uint32(t.MaxRetry))
	offset += 4
	binary.BigEndian.PutUint64(buf[offset:], uint64(t.TimeoutMs))
	offset += 8

	writeString16(t.UniqueKey)
	binary.BigEndian.PutUint64(buf[offset:], uint64(t.UniqueTTLMs))
	offset += 8
	writeString32(t.LastError)
	writeString16(t.CronSpec)

	binary.BigEndian.PutUint64(buf[offset:], uint64(t.CreatedAt.UnixNano()))
	offset += 8

	return buf, nil
}

// Unmarshal deserializes custom binary bytes to a Task
func (BinaryCodec) Unmarshal(data []byte, t *Task) error {
	if len(data) < 2 {
		return errors.New("binary codec: data too short")
	}
	offset := 0

	readString16 := func() (string, error) {
		if offset+2 > len(data) {
			return "", errors.New("binary codec: readString16 out of bounds")
		}
		length := int(binary.BigEndian.Uint16(data[offset:]))
		offset += 2
		if offset+length > len(data) {
			return "", errors.New("binary codec: readString16 payload out of bounds")
		}
		s := string(data[offset : offset+length])
		offset += length
		return s, nil
	}

	readString32 := func() (string, error) {
		if offset+4 > len(data) {
			return "", errors.New("binary codec: readString32 out of bounds")
		}
		length := int(binary.BigEndian.Uint32(data[offset:]))
		offset += 4
		if offset+length > len(data) {
			return "", errors.New("binary codec: readString32 payload out of bounds")
		}
		s := string(data[offset : offset+length])
		offset += length
		return s, nil
	}

	readBytes32 := func() ([]byte, error) {
		if offset+4 > len(data) {
			return nil, errors.New("binary codec: readBytes32 out of bounds")
		}
		length := int(binary.BigEndian.Uint32(data[offset:]))
		offset += 4
		if offset+length > len(data) {
			return nil, errors.New("binary codec: readBytes32 payload out of bounds")
		}
		b := make([]byte, length)
		copy(b, data[offset:offset+length])
		offset += length
		return b, nil
	}

	var err error
	t.ID, err = readString16()
	if err != nil {
		return err
	}
	t.Queue, err = readString16()
	if err != nil {
		return err
	}
	t.Name, err = readString16()
	if err != nil {
		return err
	}
	t.Payload, err = readBytes32()
	if err != nil {
		return err
	}

	if offset+4 > len(data) {
		return errors.New("binary codec: read Retry out of bounds")
	}
	t.Retry = int(binary.BigEndian.Uint32(data[offset:]))
	offset += 4

	if offset+4 > len(data) {
		return errors.New("binary codec: read MaxRetry out of bounds")
	}
	t.MaxRetry = int(binary.BigEndian.Uint32(data[offset:]))
	offset += 4

	if offset+8 > len(data) {
		return errors.New("binary codec: read TimeoutMs out of bounds")
	}
	t.TimeoutMs = int(binary.BigEndian.Uint64(data[offset:]))
	offset += 8

	t.UniqueKey, err = readString16()
	if err != nil {
		return err
	}

	if offset+8 > len(data) {
		return errors.New("binary codec: read UniqueTTLMs out of bounds")
	}
	t.UniqueTTLMs = int(binary.BigEndian.Uint64(data[offset:]))
	offset += 8

	t.LastError, err = readString32()
	if err != nil {
		return err
	}
	t.CronSpec, err = readString16()
	if err != nil {
		return err
	}

	if offset+8 > len(data) {
		return errors.New("binary codec: read CreatedAt out of bounds")
	}
	unixNano := int64(binary.BigEndian.Uint64(data[offset:]))
	t.CreatedAt = time.Unix(0, unixNano)
	offset += 8

	return nil
}

// unsafeStringToBytes converts a string to a byte slice without allocations.
// The returned byte slice is read-only and must not be modified.
func unsafeStringToBytes(s string) []byte {
	if s == "" {
		return nil
	}
	return unsafe.Slice(unsafe.StringData(s), len(s))
}
