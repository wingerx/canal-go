package parse

import (
	"flag"
	"fmt"
	"github.com/golang/glog"
	. "github.com/woqutech/drt/events"
	"os"
	"testing"
)

func TestMain(m *testing.M) {
	flag.Set("alsologtostderr", "true")
	//flag.Set("log_dir", "/tmp")
	flag.Set("v", "3")
	flag.Parse()

	ret := m.Run()
	os.Exit(ret)
}

func TestNewLogDecoder(t *testing.T) {
	ld := NewLogDecoder(UNKNOWN_EVENT, ENUM_END_EVENT)
	fmt.Println(ld)
}

func TestNewEmptyLogDecoder(t *testing.T) {
	ld := NewEmptyLogDecoder()
	ld.handle(ROTATE_EVENT)
	ld.handle(FORMAT_DESCRIPTION_EVENT)
	ld.handle(QUERY_EVENT)
	ld.handle(XID_EVENT)
	fmt.Println(ld)
}

func TestLogDecoder_Decode(t *testing.T) {
	// RotateEvent
	re := []byte{0x0, 0x0, 0x0, 0x0, 0x4, 0x1, 0x0, 0x0, 0x0, 0x2f, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x20, 0x0, 0x3, 0x1, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x6d, 0x79, 0x73, 0x71, 0x6c, 0x2d, 0x62, 0x69, 0x6e, 0x2e, 0x30, 0x30, 0x30, 0x30, 0x31, 0x31, 0xfd, 0xe2, 0xdc, 0xdb}
	// FormatDescriptionEvent
	fe := []byte{0x96, 0x44, 0xc6, 0x5a, 0xf, 0x1, 0x0, 0x0, 0x0, 0x77, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x4, 0x0, 0x35, 0x2e, 0x37, 0x2e, 0x32, 0x30, 0x2d, 0x6c, 0x6f, 0x67, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x13, 0x38, 0xd, 0x0, 0x8, 0x0, 0x12, 0x0, 0x4, 0x4, 0x4, 0x4, 0x12, 0x0, 0x0, 0x5f, 0x0, 0x4, 0x1a, 0x8, 0x0, 0x0, 0x0, 0x8, 0x8, 0x8, 0x2, 0x0, 0x0, 0x0, 0xa, 0xa, 0xa, 0x2a, 0x2a, 0x0, 0x12, 0x34, 0x0, 0x1, 0x9d, 0x12, 0x94, 0xf7}
	// QueryEvent
	qe := []byte{0x56, 0x89, 0xc7, 0x5a, 0x2, 0x1, 0x0, 0x0, 0x0, 0x4b, 0x0, 0x0, 0x0, 0x4e, 0x1, 0x0, 0x0, 0x8, 0x0, 0x10, 0x25, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x7, 0x0, 0x0, 0x1a, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1, 0x20, 0x0, 0xa0, 0x55, 0x0, 0x0, 0x0, 0x0, 0x6, 0x3, 0x73, 0x74, 0x64, 0x4, 0x21, 0x0, 0x21, 0x0, 0x8, 0x0, 0x65, 0x78, 0x61, 0x6d, 0x70, 0x6c, 0x65, 0x0, 0x42, 0x45, 0x47, 0x49, 0x4e, 0xbe, 0x9, 0xf5, 0x7b}
	// TableMapEvent
	te := []byte{0x56, 0x89, 0xc7, 0x5a, 0x13, 0x1, 0x0, 0x0, 0x0, 0x36, 0x0, 0x0, 0x0, 0x84, 0x1, 0x0, 0x0, 0x0, 0x0, 0x70, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1, 0x0, 0x7, 0x65, 0x78, 0x61, 0x6d, 0x70, 0x6c, 0x65, 0x0, 0x4, 0x74, 0x65, 0x73, 0x74, 0x0, 0x2, 0x11, 0xf, 0x3, 0x0, 0xc0, 0x0, 0x3, 0xfd, 0xe8, 0xd9, 0xb9}
	// WriteRowsEventV2
	we := []byte{0x56, 0x89, 0xc7, 0x5a, 0x1e, 0x1, 0x0, 0x0, 0x0, 0x26, 0x0, 0x0, 0x0, 0xaa, 0x1, 0x0, 0x0, 0x0, 0x0, 0x70, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1, 0x0, 0x2, 0x0, 0x2, 0xff, 0xfd, 0x1, 0x32, 0x81, 0x8, 0xe8, 0x91}

	ld := NewLogDecoder(UNKNOWN_EVENT, ENUM_END_EVENT)
	event, err := ld.Decode(re)
	ld.Decode(fe)
	ld.Decode(qe)
	ld.Decode(we)

	event, err = ld.Decode(te)
	if err != nil {
		glog.Error(err)
	}

	switch e := event.Event.(type) {
	case *TableMapEvent:
		glog.Infof("tID:%d, dbName: %s,tName: %s ", e.TableID, string(e.DatabaseName), string(e.TableName))
		for _, col := range e.ColumnTypes {
			glog.Infof("%v", col)
		}
	}
}
