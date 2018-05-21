package tsdb

import "testing"

func TestNewOnceConn(t *testing.T) {
	config := new(MysqlConfig)
	config.Address = "127.0.0.1"
	config.Port = "3306"
	config.Username = "root"
	config.Password = "123456"

	NewOnceConn(config)
}
