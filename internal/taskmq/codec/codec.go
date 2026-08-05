package codec

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"time"
	"unsafe"

	"github.com/twn39/taskmq/internal/taskmq/task"
)

// Codec defines the contract for task serialization and deserialization
type Codec interface {
	Marshal(t *task.Task) ([]byte, error)
	Unmarshal(data []byte, t *task.Task) error
}

// JSONCodec implements the Codec interface using standard JSON encoding
type JSONCodec struct{}

// Marshal serializes a Task to JSON bytes
func (JSONCodec) Marshal(t *task.Task) ([]byte, error) {
	return json.Marshal(t)
}

// Unmarshal deserializes JSON bytes to a Task
func (JSONCodec) Unmarshal(data []byte, t *task.Task) error {
	return json.Unmarshal(data, t)
}

// BinaryCodec implements a high-performance, schema-free, zero-allocation custom binary format.
// It maps the Task fields directly using BigEndian byte encoding.
type BinaryCodec struct{}

// Marshal serializes a Task to custom binary bytes
func (BinaryCodec) Marshal(t *task.Task) ([]byte, error) {
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
	// Trailing optional fields (v2): UniqueScope, GroupKey, DeadlineMs
	size += 4 // UniqueScope int32
	size += 2 + len(t.GroupKey)
	size += 8 // DeadlineMs int64

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

	binary.BigEndian.PutUint32(buf[offset:], uint32(t.UniqueScope))
	offset += 4
	writeString16(t.GroupKey)
	binary.BigEndian.PutUint64(buf[offset:], uint64(t.DeadlineMs))
	offset += 8

	return buf, nil
}

// Unmarshal deserializes custom binary bytes to a Task
func (BinaryCodec) Unmarshal(data []byte, t *task.Task) error {
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

	// Optional trailing fields (backward compatible with older payloads).
	if offset+4 <= len(data) {
		t.UniqueScope = task.UniqueScope(binary.BigEndian.Uint32(data[offset:]))
		offset += 4
	}
	if offset+2 <= len(data) {
		t.GroupKey, err = readString16()
		if err != nil {
			return err
		}
	}
	if offset+8 <= len(data) {
		t.DeadlineMs = int64(binary.BigEndian.Uint64(data[offset:]))
		offset += 8
	}

	return nil
}

// UnsafeStringToBytes converts a string to a byte slice without allocations.
// The returned byte slice is read-only and must not be modified.
func UnsafeStringToBytes(s string) []byte {
	if s == "" {
		return nil
	}
	return unsafe.Slice(unsafe.StringData(s), len(s))
}

// PayloadInspector is an optional interface that Codecs can implement to provide
// high-performance, schema-specific extraction of a field from task payloads.
type PayloadInspector interface {
	InspectField(payload []byte, field string) (string, error)
}

// InspectJSONField is the default streaming JSON field inspector.
func InspectJSONField(payload []byte, field string) (string, error) {
	if len(payload) == 0 || field == "" {
		return "", nil
	}

	dec := json.NewDecoder(bytes.NewReader(payload))
	tok, err := dec.Token()
	if err != nil || tok != json.Delim('{') {
		return "", errors.New("json codec: payload is not a JSON object")
	}

	for dec.More() {
		tok, err := dec.Token()
		if err != nil {
			break
		}
		key, ok := tok.(string)
		if !ok {
			continue
		}

		if key == field {
			var val interface{}
			if err := dec.Decode(&val); err == nil {
				return fmt.Sprintf("%v", val), nil
			}
			break
		}

		var skip interface{}
		if err := dec.Decode(&skip); err != nil {
			break
		}
	}
	return "", fmt.Errorf("json codec: field %q not found in payload", field)
}

// InspectField implements PayloadInspector for JSONCodec.
func (JSONCodec) InspectField(payload []byte, field string) (string, error) {
	return InspectJSONField(payload, field)
}

// InspectField implements PayloadInspector for BinaryCodec.
func (BinaryCodec) InspectField(payload []byte, field string) (string, error) {
	if len(payload) == 0 || field == "" {
		return "", nil
	}
	if payload[0] == '{' {
		return InspectJSONField(payload, field)
	}
	return "", errors.New("binary codec: cannot inspect non-JSON payload without a schema")
}
