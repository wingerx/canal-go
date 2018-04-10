package parse

import (
	"testing"
	log "github.com/golang/glog"
	. "github.com/woqutech/drt/events"
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
	auth.ReadTimeout = 10

	mc, err := NewMySQLConnection(auth, 123456)
	defer mc.Disconnect()

	if err != nil {
		log.Error(err)
		return
	}
	err = mc.Connect()
	if err != nil {
		log.Error(err)
		return
	}
	err = mc.dump("mysql-bin.000011", 259, func(event *LogEvent) bool {
		log.Info(event.Header.Type, event.Header.LogPos)
		parse(event)
		return true
	})
	if err != nil {
		log.Error(err)
		return
	}

}
func parse(event *LogEvent) {
	switch e := event.Event.(type) {
	case *RotateEvent:
		fmt.Println(string(e.NextFileName), e.NextPosition)
	case *QueryEvent:
		fmt.Println(string(e.DatabaseName))
	case *XidEvent:
		fmt.Println(e.Xid)
	case *RowsEvent:
		fmt.Println(e.Table)
	case *RowsQueryEvent:
		fmt.Println(e.RowsQuery)
	default:
		fmt.Println("###")
	}

}
