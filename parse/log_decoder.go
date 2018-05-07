package parse

import (
	"github.com/juju/errors"
	. "github.com/woqutech/drt/events"
)

type LogDecoder struct {
	handleSet map[LogEventType]bool

	format *FormatDescriptionEvent
	tables map[uint64]*TableMapEvent
}

func NewEmptyLogDecoder() *LogDecoder {
	ld := new(LogDecoder)
	ld.tables = make(map[uint64]*TableMapEvent)
	ld.format = NewFormatDescriptionEventV4()
	ld.handleSet = make(map[LogEventType]bool)
	return ld
}
func NewLogDecoder(from, to LogEventType) *LogDecoder {
	ld := new(LogDecoder)
	ld.tables = make(map[uint64]*TableMapEvent)
	ld.format = NewFormatDescriptionEventV4()
	ld.initHandleSet(from, to)
	return ld
}

func NewDumpEventLogDecoder() *LogDecoder {
	return NewLogDecoder(UNKNOWN_EVENT, ENUM_END_EVENT)
}

func NewSinkEventLogDecoder() *LogDecoder {
	ld := NewEmptyLogDecoder()
	ld.handle(ROTATE_EVENT)
	ld.handle(FORMAT_DESCRIPTION_EVENT)
	ld.handle(QUERY_EVENT)
	ld.handle(XID_EVENT)
	return ld
}

func (ld *LogDecoder) initHandleSet(from, to LogEventType) {
	ld.handleSet = make(map[LogEventType]bool)
	for i := from; i <= to; i++ {
		ld.handleSet[i] = true
	}
}

func (ld *LogDecoder) handle(flag LogEventType) {
	ld.handleSet[flag] = true
}

func (ld *LogDecoder) Decode(data []byte) (*LogEvent, error) {
	header, err := NewEventHeader(data)
	if err != nil {
		return nil, err
	}

	payloadBody := data[EventHeaderSize:]
	eventLen := int(header.EventSize) - EventHeaderSize

	if len(payloadBody) != eventLen {
		return nil, errors.New("invalid event size")
	}

	if ld.handleSet[header.Type] {
		event, err := ld.decode(header, payloadBody)
		if err != nil {
			return nil, err
		}

		return &LogEvent{Header: header, Event: event, EventBody: payloadBody}, nil
	}

	return &UnknownLogEvent{Header: header, Event: nil, EventBody: payloadBody}, nil
}

func (ld *LogDecoder) decode(header *EventHeader, data []byte) (event Event, err error) {
	if header.Type != FORMAT_DESCRIPTION_EVENT {
		// remove checksum bytes
		if ld.format.ChecksumAlgorithm != BINLOG_CHECKSUM_ALG_OFF && ld.format.ChecksumAlgorithm != BINLOG_CHECKSUM_ALG_UNDEF {
			data = data[0 : len(data)-4]
		}
	}

	switch header.Type {
	case GTID_LOG_EVENT, ANONYMOUS_GTID_LOG_EVENT:
		event, err = NewGTIDEvent(data)
	case QUERY_EVENT:
		event, err = NewQueryLogEvent(data)
	case XID_EVENT:
		event, err = NewXidEvent(data)
	case FORMAT_DESCRIPTION_EVENT:
		event, err = NewFormatDescriptionEvent(data)
		if err == nil {
			ld.format = event.(*FormatDescriptionEvent)
		}
	case ROTATE_EVENT:
		event, err = NewRotateEvent(data)
		if err == nil {
			// reset tables after a rotate event
			ld.tables = make(map[uint64]*TableMapEvent)
		}
	case TABLE_MAP_EVENT:
		event, err = NewTableMapEvent(ld.format, data)
		if err == nil {
			ld.tables[event.(*TableMapEvent).TableID] = event.(*TableMapEvent)
		}
	case WRITE_ROWS_EVENT_V1, DELETE_ROWS_EVENT_V1, UPDATE_ROWS_EVENT_V1,
		WRITE_ROWS_EVENT_V2, DELETE_ROWS_EVENT_V2, UPDATE_ROWS_EVENT_V2:
		event, err = NewRowsEvent(ld.format, ld.tables, header.Type, data)
	case EXECUTE_LOAD_QUERY_EVENT:
		event, err = NewExecuteLoadQueryEvent(data)
	case BEGIN_LOAD_QUERY_EVENT:
		event, err = NewBeginLoadQueryEvent(data)
	case ROWS_QUERY_EVENT:
		event, err = NewRowsQueryEvent(data)
	case USER_VAR_EVENT:
		event, err = NewUserVarLogEvent(data)
	case INTVAR_EVENT:
		event, err = NewIntVarEvent(data)
	case HEARTBEAT_EVENT:
		event, err = NewHeartbeatLogEvent(data)
	case RAND_EVENT:
		event, err = NewRandLogEvent(data)
	default:
		event, err = NewUnNamedLogEvent(data)
	}

	if err != nil {
		return nil, &EventError{Header: header, Err: err.Error(), Data: data}
	}

	if re, ok := event.(*RowsEvent); ok {
		if (re.Flags & RowsEventStmtEndFlag) > 0 {
			// reset tables after a rotate event
			ld.tables = make(map[uint64]*TableMapEvent)
		}
	}
	return event, nil
}
