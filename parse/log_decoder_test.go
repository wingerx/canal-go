package parse

import (
	"testing"
	"flag"
	"os"
	. "github.com/woqutech/drt/events"
	"fmt"
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
	ld := NewLogDecoder(UNKNOWN_EVENT, PREVIOUS_GTIDS_LOG_EVENT)
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
