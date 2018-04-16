package parse

import (
	"github.com/golang/glog"
	. "github.com/woqutech/drt/events"
	"testing"
)

func TestNewMySQLConnection(t *testing.T) {
	auth := new(AuthenticInfo)
	auth.host = "127.0.0.1"
	auth.port = 3306
	auth.username = "root"
	auth.password = "123456"
	auth.charset = "utf8"
	//auth.connTimeout = 10
	auth.ReadTimeout = 10

	mc := NewMySQLConnection(auth, 123456)

	err := mc.Connect()
	if err != nil {
		glog.Error(err)
		return
	}
	err = mc.dump("mysql-bin.000011", 259, func(event *LogEvent) bool {
		glog.Info(event.Header.Type, event.Header.LogPos)
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
		glog.Info(string(e.NextFileName), e.NextPosition)
	case *QueryEvent:
		glog.Info(string(e.DatabaseName))
	case *TableMapEvent:
		glog.Info(e.TableID, string(e.DatabaseName), e.TableName)
	case *XidEvent:
		glog.Info(e.Xid)
	case *RowsEvent:
		glog.Info(e.Table)
	case *RowsQueryEvent:
		glog.Info(e.RowsQuery)
	default:
		glog.Info(e)
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
	auth.ReadTimeout = 10

	mc := NewMySQLConnection(auth, 123456)
	err := mc.Connect()
	if err != nil {
		glog.Error(err)
		return
	}
	defer mc.Disconnect()
}
