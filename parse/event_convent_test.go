package parse

import (
	"testing"
	"github.com/golang/glog"
	"github.com/wingerx/drt/events"
	"github.com/juju/errors"
	"github.com/wingerx/drt/protoc"
	"github.com/gogo/protobuf/proto"
)

func TestNewEventCovert(t *testing.T) {
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
	tblCache, err := NewTableMetaCache(mc)
	if err != nil {
		glog.Error(err)
		return
	}

	ec := NewEventCovert(tblCache)

	sinkFunc := func(event *events.LogEvent) bool {
		glog.Infof("event type: %v, pos: %v, offset: %v", event.Header.Type, event.Header.LogPos, int32(event.Header.LogPos-event.Header.EventSize))
		entry, err := ec.Parse(event)
		if err != nil && errors.Cause(err) != IgnoreEventErr {
			glog.Error(errors.Trace(err))
			return false
		}

		if entry != nil {
			switch entry.EntryType {
			case protoc.EntryType_TRANSACTION_BEGIN:
				var tb protoc.TransactionBegin
				glog.Infof("error: %v, value: %s", proto.Unmarshal(entry.StoreValue, &tb), tb.String())
			case protoc.EntryType_ROWDATA:
				var rc protoc.RowChange
				err := proto.Unmarshal(entry.StoreValue, &rc)
				glog.Infof("error: %v, value: %s", err, rc.String())
			case protoc.EntryType_TRANSACTION_END:
				var te protoc.TransactionEnd
				glog.Infof("error: %v, value: %s", proto.Unmarshal(entry.StoreValue, &te), te.String())
			}
		}

		return true
	}

	err = mc.Dump("mysql-bin.000017", 194, sinkFunc)
	if err != nil {
		glog.Error(err)
		return
	}

}
