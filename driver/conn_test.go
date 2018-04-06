package driver

import (
	"testing"
	"flag"
	"os"
	"context"
	"github.com/stretchr/testify/assert"
)

func TestMain(m *testing.M) {
	flag.Set("alsologtostderr", "true")
	//flag.Set("log_dir", "/tmp")
	flag.Set("v", "3")
	flag.Parse()

	ret := m.Run()
	os.Exit(ret)
}

func TestMySQLConnector_Connect(t *testing.T) {
	var out = ""
	// 正常连接，无默认db
	ctx := context.Background()
	mc := NewMySQLConnector("127.0.0.1:3306", "root", "123456", "")
	err := mc.Connect(ctx)
	if err != nil {
		out = err.Error()
	}
	mc.Close()
	assert.Equal(t, "", out)

	// 错误密码
	mc.password = "wrong password"
	want := "ErrorPacket [errorCode=1045, message=Access denied for user 'root'@'172.17.0.1' (using password: YES), sqlState=28000]"
	err = mc.Connect(ctx)
	if err != nil {
		out = err.Error()
	}
	assert.Equal(t, want, out)
	mc.Close()

	// 错误数据库名
	mc.password = "123456"
	mc.dbname = "notexistdb"
	want = "ErrorPacket [errorCode=1049, message=Unknown database 'notexistdb', sqlState=42000]"
	out = ""
	err = mc.Connect(ctx)
	if err != nil {
		out = err.Error()
	}
	assert.Equal(t, want, out)
	mc.Close()

	// 正确数据库名
	mc.dbname = "mysql"
	out = ""
	err = mc.Connect(ctx)
	if err != nil {
		out = err.Error()
	}
	assert.Equal(t, "", out)
	mc.Close()
}
