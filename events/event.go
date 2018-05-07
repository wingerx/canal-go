package events

import (
	"encoding/binary"
	"github.com/juju/errors"
)

// Event represents the real data in an LogEvent, in a possibly-parsed form.
type Event interface {
}

// An LogEvent represents a single event from a raw MySQL binlog stream.
// A Streamer receives these events through a Follower and processes them.
type LogEvent struct {
	Header    *EventHeader // parsed event header
	Event     Event        // parsed event body
	EventBody []byte       // event body as raw bytes
}

type UnknownLogEvent = LogEvent

type EventError struct {
	Header *EventHeader
	Err    string
	Data   []byte
}

func (e *EventError) Error() string {
	return e.Err
}

type EventHeader struct {
	Timestamp uint32
	Type      LogEventType
	ServerId  uint32
	EventSize uint32
	LogPos    uint32
	Flags     uint16
}

// Returns a parsed EventHeader if the bytes provided are valid.
// (This requires binlog version 4.0+)
func NewEventHeader(data []byte) (*EventHeader, error) {
	if len(data) < EventHeaderSize {
		return nil, errors.Errorf("header size too short, required [%d] but get [%d]", EventHeaderSize, len(data))
	}

	header := new(EventHeader)

	offset := 0

	// Seconds since UNIX epoch
	header.Timestamp = binary.LittleEndian.Uint32(data[offset : offset+4])
	offset += 4

	// MySQL-defined binlog event type
	header.Type = LogEventType(data[offset])
	offset++

	// ID of the originating MySQL server; used to filter out events in circular replication
	header.ServerId = binary.LittleEndian.Uint32(data[offset : offset+4])
	offset += 4

	// Event size, including header, post-header, and body
	eventSize := binary.LittleEndian.Uint32(data[offset:])
	if eventSize < uint32(EventHeaderSize) {
		return nil, errors.Errorf("invalid event size %d, must >= %d", eventSize, EventHeaderSize)
	}
	header.EventSize = eventSize
	offset += 4

	// Position of the next event
	header.LogPos = binary.LittleEndian.Uint32(data[offset : offset+4])
	offset += 4

	// MySQL-defined binlog event flag
	header.Flags = binary.LittleEndian.Uint16(data[offset : offset+2])
	offset += 2

	return header, nil
}

// used to record all unnamed events
type UnNamedLogEvent struct {
	Data []byte
}

func NewUnNamedLogEvent(data []byte) (Event, error) {
	return &UnNamedLogEvent{data}, nil
}
