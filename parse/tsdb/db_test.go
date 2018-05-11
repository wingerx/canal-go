package tsdb

import "testing"

func TestNewOnceConn(t *testing.T) {
	info := new(Info)
	info.Address = "127.0.0.1"
	info.Port = "3306"
	info.Username = "root"
	info.Password = "123456"

	NewOnceConn(info)
}
