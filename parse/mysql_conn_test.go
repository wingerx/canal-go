package parse

import (
	"github.com/golang/glog"
	. "github.com/wingerx/drt/events"
	"testing"
	"github.com/satori/go.uuid"
	"fmt"
)

func TestNewMySQLConnection(t *testing.T) {
	auth := new(AuthenticInfo)
	auth.host = "127.0.0.1"
	auth.port = 3306
	auth.username = "root"
	auth.password = "123456"
	auth.charset = "utf8"
	//auth.connTimeout = 10
	auth.readTimeout = 10

	mc := NewMySQLConnection(auth, 123456)

	if err := mc.Connect(); err != nil {
		glog.Error(err)
		return
	}

	err := mc.Dump("mysql-bin.000017", 194, func(event *LogEvent) bool {
		glog.Infof("event type: %v, pos: %v, offset: %v", event.Header.Type, event.Header.LogPos, int32(event.Header.LogPos-event.Header.EventSize))
		parse(event)
		return true
	})
	if err != nil {
		glog.Error(err)
		return
	}

}
func parse(event *LogEvent) {
	switch e := event.Event.(type) {
	case *RotateEvent:
		//glog.Info(string(e.NextFileName), e.NextPosition)
	case *QueryEvent:
		//glog.Info(string(e.DatabaseName))
	case *TableMapEvent:
		//glog.Info(e.TableID, string(e.DatabaseName), e.TableName)
	case *XidEvent:
		//glog.Info(e.Xid)
	case *RowsEvent:
		glog.Infof("column types: %v", e.Table.ColumnTypes)
		glog.Infof("column count: %v", e.Table.ColumnCount)
		for i, r := range e.Rows {
			if (i+1)%2 == 0 && event.Header.Type == UPDATE_ROWS_EVENT_V2 {
				glog.Infof("after rows: %v", r)
			} else {
				glog.Infof("before rows: %v", r)
			}

		}
		//glog.Info(e.Table)
	case *GTIDEvent:
		glog.Infof("lastCommitted : %v, seqNum: %v", e.LastCommitted, e.SequenceNumber)

		u, _ := uuid.FromBytes(e.SID)
		gtid := fmt.Sprintf("%s:%d", u.String(), e.GNO)
		gset, _ := ParseGTIDSet(gtid)
		glog.Infof("before gset: %v", gset)
		gset.Update(gtid)
		glog.Infof("after gset: %v", gset)

	case *RowsQueryEvent:
		//glog.Info(e.RowsQuery)
	default:
		//glog.Info(e)
	}

}

func TestMySQLConnection_Disconnect(t *testing.T) {
	auth := new(AuthenticInfo)
	auth.host = "127.0.0.1"
	auth.port = 3306
	auth.username = "root"
	auth.password = "123456"
	auth.charset = "utf8"
	//auth.connTimeout = 10
	auth.readTimeout = 10

	mc := NewMySQLConnection(auth, 123456)
	err := mc.Connect()
	if err != nil {
		glog.Error(err)
		return
	}
	defer mc.Disconnect()
}
