package parse

import (
	"fmt"
	"github.com/gogo/protobuf/proto"
	"github.com/golang/glog"
	"github.com/juju/errors"
	"github.com/satori/go.uuid"
	. "github.com/woqutech/drt/events"
	. "github.com/woqutech/drt/protoc"
	"strings"
)

var IgnoreEventErr = errors.New("ignore event")

const (
	TRANSACTION_BEGIN = "BEGIN"
	TRANSACTION_END   = "COMMIT"
)

type EventConvert struct {
	binlogName string
	// gtid
	gset          GTIDSet
	lastCommitted int64
	sequenceNum   int64
}

//	if err != nil && errors.Cause(err) != IgnoreEventErr {
//		return errors.Trace(err)
//	}
func (ec *EventConvert) Parse(event *LogEvent) (entry *Entry, err error) {

	switch e := event.Event.(type) {
	case *UnknownLogEvent:
		err = IgnoreEventErr
		break
	case *RotateEvent:
		ec.binlogName = string(e.NextFileName)
		break
	case *GTIDEvent:
		ec.lastCommitted = e.LastCommitted
		ec.sequenceNum = e.SequenceNumber

		u, _ := uuid.FromBytes(e.SID)
		gtid := fmt.Sprintf("%s:%d", u.String(), e.GNO)
		if ec.gset == nil {
			ec.gset, _ = ParseGTIDSet(gtid)
		} else {
			err := ec.gset.Update(gtid)
			if err != nil {
				glog.Errorf("gtid set update err: %v", err)
				return nil, errors.Trace(err)
			}
		}
		break
	case *QueryEvent:
		entry, err = ec.parseQueryEvent(event.Header, e)
	case *XidEvent:
		entry, err = ec.parseXidEvent(event.Header, e)
		break
	case *TableMapEvent:
		err = IgnoreEventErr
		break
	case *RowsEvent:
		entry, err = ec.parseRowsEvent(event.Header, e)
		break
	case *RowsQueryEvent:
		break
	case *UserVarLogEvent:
		break
	case *IntVarEvent:
		break
	case *RandLogEvent:
		break
	default:
		err = IgnoreEventErr
		glog.Infof("pass ignore event:%v", e)
		break
	}

	return entry, err
}

func (ec *EventConvert) parseQueryEvent(head *EventHeader, qe *QueryEvent) (*Entry, error) {
	query := string(qe.Query)

	if strings.HasPrefix(strings.ToUpper(query), TRANSACTION_BEGIN) {
		tb := &TransactionBegin{
			ThreadId: int64(qe.ThreadId),
		}
		h := ec.createHeader(head, string(qe.DatabaseName), "", EventType_QUERY)
		data, _ := proto.Marshal(tb)
		return createEntry(h, EntryType_TRANSACTION_BEGIN, data), nil
	} else if strings.HasPrefix(strings.ToUpper(query), TRANSACTION_END) {
		te := &TransactionEnd{
			TransactionId: "0",
		}
		h := ec.createHeader(head, string(qe.DatabaseName), "", EventType_QUERY)
		data, _ := proto.Marshal(te)
		return createEntry(h, EntryType_TRANSACTION_END, data), nil
	} else {
		var dbName, tblName string
		eventType := EventType_QUERY

		sqlParser := NewSQLParser(string(qe.Query))
		ddlResults, err := sqlParser.Parse(string(qe.DatabaseName))
		if err != nil {
			return nil, err
		}
		// TODO ADD dbName.tblName 过滤

		// 针对多行 ddl，只处理第一条
		if len(ddlResults) > 0 {
			eventType = ddlResults[0].eventType
			tblName = ddlResults[0].tableName
			dbName = ddlResults[0].schemaName
		}
		h := ec.createHeader(head, dbName, tblName, eventType)
		rc := new(RowChange)
		if eventType != EventType_QUERY {
			rc.IsDDL = true
		}
		rc.Sql = query
		rc.DdlSchemaName = dbName
		rc.EventType = eventType
		data, _ := proto.Marshal(rc)
		return createEntry(h, EntryType_ROWDATA, data), nil
	}
}

func (ec *EventConvert) parseXidEvent(head *EventHeader, xe *XidEvent) (*Entry, error) {
	te := &TransactionEnd{
		TransactionId: string(xe.Xid),
	}

	h := ec.createHeader(head, "", "", EventType_QUERY)
	data, _ := proto.Marshal(te)
	return createEntry(h, EntryType_TRANSACTION_END, data), nil
}

func (ec *EventConvert) parseRowsEvent(head *EventHeader, re *RowsEvent) (*Entry, error) {
	tblEvent := re.Table
	if tblEvent == nil {
		return nil, ErrMissingTableId(errors.Errorf("not found table id %d", re.TableID))
	}
	dbName := string(re.Table.DatabaseName)
	tblName := string(re.Table.TableName)
	//fullName := fmt.Sprintf("%v.%v", dbName, tblName)

	var eventType EventType
	switch head.Type {
	case WRITE_ROWS_EVENT_V1, WRITE_ROWS_EVENT_V2:
		eventType = EventType_INSERT
	case UPDATE_ROWS_EVENT_V1, UPDATE_ROWS_EVENT_V2:
		eventType = EventType_UPDATE
	case DELETE_ROWS_EVENT_V1, DELETE_ROWS_EVENT_V2:
		eventType = EventType_DELETE
	}

	h := ec.createHeader(head, dbName, tblName, eventType)
	rc := new(RowChange)
	rc.TableId = int64(re.TableID)
	rc.EventType = eventType
	rc.IsDDL = false

	for i := 0; i < int(re.Table.ColumnCount); i++ {
		column := new(Column)
		column.Index = int32(i)
		column.IsNull = false

	}
	//for _, row := range re.Rows {
	//
	//}
	createEntry(h, EntryType_ROWDATA, nil)
	return nil, nil
}

func (ec *EventConvert) createHeader(header *EventHeader, dbName string, tblName string, eventType EventType) *Header {
	h := new(Header)
	h.Version = 1
	h.LogfileName = ec.binlogName
	h.LogfileOffset = int64(header.LogPos - header.EventSize)
	h.ServerId = int64(header.ServerId)
	h.ServerEncode = UTF8
	h.ExecuteTime = int64(header.Timestamp) * 1000
	h.SourceType = Type_MYSQL
	h.EventType = eventType
	h.TableName = tblName
	h.SchemaName = dbName
	h.Gtid = ec.gset.String()
	h.LastCommitted = ec.lastCommitted
	h.SeqNum = ec.sequenceNum
	return h
}

func createEntry(header *Header, entryType EntryType, storeValue []byte) *Entry {
	e := new(Entry)
	e.Header = header
	e.EntryType = entryType
	e.StoreValue = storeValue
	return e
}
