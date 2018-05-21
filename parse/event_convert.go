package parse

import (
	"fmt"
	"github.com/gogo/protobuf/proto"
	"github.com/golang/glog"
	"github.com/juju/errors"
	"github.com/satori/go.uuid"
	. "github.com/wingerx/drt/events"
	. "github.com/wingerx/drt/protoc"
	. "github.com/wingerx/drt/tools"
	"strconv"
	"strings"
	"time"
	"github.com/shopspring/decimal"
	"github.com/wingerx/drt/parse/tsdb"
	"encoding/json"
)

var IgnoreEventErr = errors.New("ignore event")

const (
	TRANSACTION_BEGIN = "BEGIN"
	TRANSACTION_END   = "COMMIT"
)

type EventConvert struct {
	binlogName    string
	gset          GTIDSet
	lastCommitted int64
	sequenceNum   int64
	tableName     string
	databaseName  string

	tableMetaCache *TableMetaCache

	FilterQueryDcl bool
	FilterQueryDml bool
	FilterQueryDdl bool
	FilterTableErr bool
	FilterRows     bool
}

func NewEventCovert(tblMetaCache *TableMetaCache) *EventConvert {
	return &EventConvert{
		tableMetaCache: tblMetaCache,
	}
}

func (ec *EventConvert) Parse(event *LogEvent) (entry *Entry, err error) {

	switch e := event.Event.(type) {
	case *UnknownLogEvent:
		err = IgnoreEventErr
	case *RotateEvent:
		ec.binlogName = string(e.NextFileName)
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
	case *QueryEvent:
		entry, err = ec.parseQueryEvent(event.Header, e)
	case *XidEvent:
		entry, err = ec.parseXidEvent(event.Header, e)
	case *TableMapEvent:
		err = IgnoreEventErr
	case *RowsEvent:
		entry, err = ec.parseRowsEvent(event.Header, e)
	case *RowsQueryEvent:
		entry, err = ec.parseRowsQueryEvent(event.Header, e)
	case *UserVarLogEvent:
		entry, err = ec.parseUserVarEvent(event.Header, e)
	case *IntVarEvent:
		entry, err = ec.parseIntVarEvent(event.Header, e)
	case *RandLogEvent:
		entry, err = ec.parseRandLogEvent(event.Header, e)
	default:
		err = IgnoreEventErr
		glog.Infof("pass ignore event:%s", event.Header.Type)
	}

	return entry, err
}

func (ec *EventConvert) parseQueryEvent(evtHead *EventHeader, qe *QueryEvent) (*Entry, error) {
	query := string(qe.Query)

	if strings.HasPrefix(strings.ToUpper(query), TRANSACTION_BEGIN) {
		tb := &TransactionBegin{
			ThreadId: int64(qe.ThreadId),
		}
		h := ec.createHeader(evtHead, string(qe.DatabaseName), ec.tableName, EventType_QUERY)
		data, _ := proto.Marshal(tb)
		return createEntry(h, EntryType_TRANSACTION_BEGIN, data), nil
	} else if strings.HasPrefix(strings.ToUpper(query), TRANSACTION_END) {
		te := &TransactionEnd{
			TransactionId: "0",
		}
		h := ec.createHeader(evtHead, string(qe.DatabaseName), ec.tableName, EventType_QUERY)
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
		for _, ddl := range ddlResults {
			ec.applyDDLQuery(string(qe.Query), evtHead, ddl)
		}

		// 针对多行 ddl，只记录第一条
		if len(ddlResults) > 0 {
			eventType = ddlResults[0].eventType
			tblName = ddlResults[0].tableName
			dbName = ddlResults[0].schemaName
		}

		h := ec.createHeader(evtHead, dbName, tblName, eventType)
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

func (ec *EventConvert) parseXidEvent(evtHead *EventHeader, xe *XidEvent) (*Entry, error) {
	te := &TransactionEnd{
		TransactionId: string(xe.Xid),
	}

	h := ec.createHeader(evtHead, "", "", EventType_QUERY)
	data, _ := proto.Marshal(te)
	return createEntry(h, EntryType_TRANSACTION_END, data), nil
}

func (ec *EventConvert) parseRowsEvent(evtHead *EventHeader, re *RowsEvent) (*Entry, error) {
	tblEvent := re.Table
	if tblEvent == nil {
		return nil, ErrMissingTableId(errors.Errorf("not found table id %d", re.TableID))
	}
	ec.databaseName = string(re.Table.DatabaseName)
	ec.tableName = string(re.Table.TableName)
	fullName := fmt.Sprintf("%v.%v", ec.databaseName, ec.tableName)
	var eventType EventType
	switch evtHead.Type {
	case WRITE_ROWS_EVENT_V1, WRITE_ROWS_EVENT_V2:
		eventType = EventType_INSERT
	case UPDATE_ROWS_EVENT_V1, UPDATE_ROWS_EVENT_V2:
		eventType = EventType_UPDATE
	case DELETE_ROWS_EVENT_V1, DELETE_ROWS_EVENT_V2:
		eventType = EventType_DELETE
	}
	var tblMeta *TableMeta
	tblErr := false
	if ec.tableMetaCache != nil {
		tblMeta = ec.tableMetaCache.GetOneTableMeta(ec.databaseName, ec.tableName)
		if tblMeta == nil {
			tblErr = true
			return nil, errors.New(fmt.Sprintf("not found table %s in database", fullName))
		}
	}

	h := ec.createHeader(evtHead, ec.databaseName, ec.tableName, eventType)
	rc := new(RowChange)
	rc.TableId = int64(re.TableID)
	rc.EventType = eventType
	rc.IsDDL = false

	for i := 0; i < len(re.Rows); i++ {
		rd := new(RowData)
		switch eventType {
		case EventType_INSERT:
			// insert 处理after 字段
			if err := ec.parseOneRow(rd, re.Rows[i], re, evtHead, tblMeta, true); err != nil {
				glog.Error(err)
				tblErr = true
			}
		case EventType_UPDATE:
			// update 处理before 字段
			if err := ec.parseOneRow(rd, re.Rows[i], re, evtHead, tblMeta, false); err != nil {
				glog.Error(err)
				tblErr = true
			}
			// update 处理after 字段
			i++
			if i >= len(re.Rows) {
				rc.RowDatas = append(rc.RowDatas, rd)
				break
			}
			changeRow := re.Rows[i]
			if err := ec.parseOneRow(rd, changeRow, re, evtHead, tblMeta, true); err != nil {
				glog.Error(err)
				tblErr = true
			}
		case EventType_DELETE:
			// delete 处理before 字段
			if err := ec.parseOneRow(rd, re.Rows[i], re, evtHead, tblMeta, false); err != nil {
				glog.Error(err)
				tblErr = true
			}
		}
		rc.RowDatas = append(rc.RowDatas, rd)
	}

	if tblErr {
		return nil, errors.New(fmt.Sprintf("parse table [%s] RowsEvent error", fullName))
	}
	data, _ := proto.Marshal(rc)
	return createEntry(h, EntryType_ROWDATA, data), nil
}

func (ec *EventConvert) parseOneRow(rowData *RowData, oneRow []interface{}, re *RowsEvent, evtHead *EventHeader, tblMeta *TableMeta, isAfter bool) error {
	tblEvt := re.Table
	if tblMeta != nil && len(tblEvt.ColumnTypes) > len(tblMeta.Columns) {
		errLog.Print(fmt.Sprintf("column size is not match for table: %s.%s, %d vs %d", string(tblEvt.DatabaseName), string(tblEvt.TableName), len(tblEvt.ColumnTypes), len(tblMeta.Columns)))
		pos := ec.createPosition(evtHead)
		tm := ec.tableMetaCache.GetOneTableMetaViaTSDB(string(tblEvt.DatabaseName), string(tblEvt.TableName), pos, false)
		if tm == nil {
			return errors.New(fmt.Sprintf("not found table %s.%s in database", string(tblEvt.DatabaseName), string(tblEvt.TableName)))
		}
		// 重新判断一次
		if len(tblEvt.ColumnTypes) > len(tm.Columns) {
			return errors.New(fmt.Sprintf("column size is not match for table: %s.%s, %d vs %d", string(tblEvt.DatabaseName), string(tblEvt.TableName), len(tblEvt.ColumnTypes), len(tm.Columns)))
		}
		tblMeta = tm
	}

	for i := 0; i < int(tblEvt.ColumnCount); i++ {
		tblCol := tblMeta.findTableColumn(i)
		column := new(Column)
		column.Index = int32(i)
		column.IsNull = false
		if tblCol != nil {
			column.Name = tblCol.Name
			column.IsKey = tblMeta.isPKColumn(tblCol.Name)
			column.MysqlType = tblCol.RawType
		}

		colType := tblEvt.ColumnTypes[i]
		column.SqlType = int32(colType)
		val := oneRow[i]
		if val != nil {
			switch colType {
			case MYSQL_TYPE_NULL:
				column.IsNull = true
				column.Value = ""
			case MYSQL_TYPE_TINY:
				switch v := val.(type) {
				case uint8:
					column.Value = strconv.FormatUint(uint64(v), 10)
				case int8:
					column.Value = strconv.FormatInt(int64(v), 10)
				}
			case MYSQL_TYPE_SHORT:
				switch v := val.(type) {
				case uint16:
					column.Value = strconv.FormatUint(uint64(v), 10)
				case int16:
					column.Value = strconv.FormatInt(int64(v), 10)
				}
			case MYSQL_TYPE_INT24, MYSQL_TYPE_LONG:
				switch v := val.(type) {
				case uint32:
					column.Value = strconv.FormatUint(uint64(v), 10)
				case int32:
					column.Value = strconv.FormatInt(int64(v), 10)
				}
			case MYSQL_TYPE_LONGLONG:
				switch v := val.(type) {
				case uint64:
					column.Value = strconv.FormatUint(v, 10)
				case int64:
					column.Value = strconv.FormatInt(v, 10)
				}
			case MYSQL_TYPE_NEWDECIMAL:
				switch v := val.(type) {
				case decimal.Decimal:
					column.Value = v.String()
				case float64:
					column.Value = strconv.FormatFloat(v, 'E', -1, 64)
				}
			case MYSQL_TYPE_FLOAT:
				column.Value = strconv.FormatFloat(float64(val.(float32)), 'E', -1, 64)
			case MYSQL_TYPE_DOUBLE:
				column.Value = strconv.FormatFloat(val.(float64), 'E', -1, 64)
			case MYSQL_TYPE_BIT:
				column.Value = strconv.FormatInt(val.(int64), 10)
			case MYSQL_TYPE_TIMESTAMP, MYSQL_TYPE_DATETIME:
				column.Value = val.(time.Time).String()
			case MYSQL_TYPE_TIMESTAMP2, MYSQL_TYPE_DATETIME2:
				column.Value = val.(string)
			case MYSQL_TYPE_TIME, MYSQL_TYPE_TIME2, MYSQL_TYPE_DATE, MYSQL_TYPE_YEAR:
				column.Value = val.(string)
			case MYSQL_TYPE_ENUM, MYSQL_TYPE_SET:
				column.Value = strconv.FormatInt(val.(int64), 10)
			case MYSQL_TYPE_BLOB, MYSQL_TYPE_GEOMETRY:
				column.Value = string(val.([]byte))
			case MYSQL_TYPE_VARCHAR, MYSQL_TYPE_VAR_STRING, MYSQL_TYPE_STRING:
				column.Value = val.(string)
			case MYSQL_TYPE_JSON:
				column.Value = string(val.([]byte))
			default:
				glog.Errorf("unknown value type: %x", colType)
				return errors.New(fmt.Sprintf("unknown value type: %x", colType))
			}
		}
		var newValue interface{}
		if column.IsNull {
			newValue = nil
		} else {
			newValue = column.Value
		}
		column.Updated = isAfter && isUpdated(rowData.BeforeColumns, newValue, i)
		if isAfter {
			rowData.AfterColumns = append(rowData.AfterColumns, column)
		} else {
			rowData.BeforeColumns = append(rowData.BeforeColumns, column)
		}
	}
	return nil
}

func (ec *EventConvert) parseRowsQueryEvent(evtHead *EventHeader, rqe *RowsQueryEvent) (*Entry, error) {
	if ec.FilterQueryDml {
		return nil, nil
	}
	tblName := ec.tableName
	sql := rqe.RowsQuery
	if len(tblName) <= 0 {
		sqlParser := NewSQLParser(sql)
		ddlResults, err := sqlParser.Parse(ec.databaseName)
		if err != nil {
			return nil, err
		}
		if len(ddlResults) > 0 {
			tblName = ddlResults[0].tableName
		}
	}

	return ec.createQueryEntry(sql, evtHead, tblName), nil
}

func (ec *EventConvert) parseUserVarEvent(evtHead *EventHeader, ue *UserVarLogEvent) (*Entry, error) {
	if ec.FilterQueryDml {
		return nil, nil
	}
	return ec.createQueryEntry(ue.Query, evtHead, ec.tableName), nil
}

func (ec *EventConvert) parseIntVarEvent(evtHead *EventHeader, ie *IntVarEvent) (*Entry, error) {
	if ec.FilterQueryDml {
		return nil, nil
	}
	return ec.createQueryEntry(ie.Query, evtHead, ec.tableName), nil
}

func (ec *EventConvert) parseRandLogEvent(evtHead *EventHeader, re *RandLogEvent) (*Entry, error) {
	if ec.FilterQueryDml {
		return nil, nil
	}
	return ec.createQueryEntry(re.Query, evtHead, ec.tableName), nil
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

func (ec *EventConvert) createPosition(evtHead *EventHeader) *EntryPosition {
	return &EntryPosition{
		Included:    false,
		LogfileName: ec.binlogName,
		LogPosition: int64(evtHead.LogPos),
		Timestamp:   int64(evtHead.Timestamp * 1000),
		ServerId:    int64(evtHead.ServerId),
	}
}

func (ec *EventConvert) createQueryEntry(query string, evtHead *EventHeader, tblName string) *Entry {
	h := ec.createHeader(evtHead, ec.databaseName, tblName, EventType_QUERY)
	rc := new(RowChange)
	rc.Sql = query
	rc.EventType = EventType_QUERY

	data, _ := proto.Marshal(rc)
	return createEntry(h, EntryType_ROWDATA, data)
}

func createEntry(header *Header, entryType EntryType, storeValue []byte) *Entry {
	e := new(Entry)
	e.Header = header
	e.EntryType = entryType
	e.StoreValue = storeValue
	return e
}

func isUpdated(bfCols []*Column, newValue interface{}, index int) bool {
	for _, v := range bfCols {
		if v.Index == int32(index) {
			if v.IsNull && newValue == nil {
				return false
			} else if newValue != nil && !v.IsNull && newValue.(string) == v.Value {
				return false
			}
		}
	}
	return true
}

func (ec *EventConvert) applyDDLQuery(query string, evtHead *EventHeader, result *DdlParserResult) {
	if ec.tableMetaCache != nil && (result.eventType == EventType_ALTER || result.eventType == EventType_DROP || result.eventType == EventType_RENAME || result.eventType == EventType_CREATE) {
		tmc := ec.tableMetaCache
		tm := tmc.GetOneTableMeta(result.schemaName, result.oriTableName)

		// 将当前的表结构信息存入
		if tm != nil {
			position := ec.createPosition(evtHead)
			_, err := tsdb.SelectLatestTableMeta(tmc.destination, result.schemaName, result.oriTableName, position.Timestamp)
			if err != nil {
				tableMetaValue, err := json.Marshal(tm)
				if err != nil {
					glog.Errorf("sql [%s] parse error: %v", query, errors.Trace(err))
					return
				}
				meta := new(tsdb.MetaHistory)
				meta.DdlType = "INIT"
				meta.SqlText = query
				meta.TableMetaValue = string(tableMetaValue)
				meta.Destination = tmc.destination
				meta.LogfileName = "0"
				meta.LogPosition = 0
				meta.ExecuteTime = 0
				meta.SchemaName = result.schemaName
				meta.TableName = result.oriTableName
				meta.ServerId = strconv.FormatInt(position.ServerId, 10)
				// 插入数据库
				tsdb.InsertTableMeta(meta)
			}
		}

		tblName := result.oriTableName
		if result.eventType == EventType_RENAME {
			tblName = result.tableName
		}
		// 重新获取变更后的表结构信息
		tmc.RestoreOneTableMeta(result.schemaName, tblName)
		tm = tmc.GetOneTableMeta(result.schemaName, tblName)
		// 发生ddl 操作，将变化后的表结构信息持久化 tsdb
		if tm != nil {
			position := ec.createPosition(evtHead)

			tableMetaValue, err := json.Marshal(tm)
			if err != nil {
				glog.Errorf("sql [%s] parse error: %v", query, errors.Trace(err))
				return
			}
			meta := new(tsdb.MetaHistory)
			meta.DdlType = result.eventType.String()
			meta.SqlText = query
			meta.TableMetaValue = string(tableMetaValue)
			meta.Destination = tmc.destination
			meta.LogfileName = position.LogfileName
			meta.LogPosition = position.LogPosition
			meta.ExecuteTime = position.Timestamp
			meta.SchemaName = result.schemaName
			meta.TableName = result.oriTableName
			meta.ServerId = strconv.FormatInt(position.ServerId, 10)
			// 插入数据库
			tsdb.InsertTableMeta(meta)
		}
	}
}
