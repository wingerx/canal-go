package tsdb

import (
	"testing"
	"flag"
	"os"
)

func TestMain(m *testing.M) {
	flag.Set("alsologtostderr", "true")
	flag.Set("v", "3")
	flag.Parse()

	ret := m.Run()
	os.Exit(ret)
}

func TestInitTableMeta(t *testing.T) {
	initConn()
	InitTableMeta()
}

func initConn() {
	info := new(Info)
	info.Address = "127.0.0.1"
	info.Port = "3306"
	info.Username = "root"
	info.Password = "123456"

	NewOnceConn(info)
}
