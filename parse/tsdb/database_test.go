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
	config := new(MysqlConfig)
	config.Address = "127.0.0.1"
	config.Port = "3306"
	config.Username = "root"
	config.Password = "123456"

	NewOnceConn(config)
}
