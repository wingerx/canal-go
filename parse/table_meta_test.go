package parse

import (
	"testing"
	"github.com/golang/glog"
	"encoding/json"
	"github.com/juju/errors"
)

func TestNewTableMetaCache(t *testing.T) {
	auth := new(AuthenticInfo)
	auth.host = "127.0.0.1"
	auth.port = 3306
	auth.username = "root"
	auth.password = "123456"
	auth.charset = "utf8"
	auth.readTimeout = 10

	mc := NewMySQLConnection(auth, 123456)
	err := mc.Connect()
	if err != nil {
		glog.Error(errors.Trace(err))
		return
	}

	tmc, err := NewTableMetaCache("example", mc)
	if err != nil {
		glog.Error(errors.Trace(err))
		return
	}
	js, err := json.Marshal(tmc.cache.tableMetas)
	if err != nil {
		glog.Error(errors.Trace(err))
		return
	}
	glog.Infof("JSON format: %s", js)

	var cache map[string]*TableMeta
	err = json.Unmarshal(js, &cache)
	if err != nil {
		glog.Error(errors.Trace(err))
		return
	}

	tmc.RestoreOneTableMeta("example", "t1")
	js, err = json.Marshal(tmc.cache.tableMetas)
	if err != nil {
		glog.Error(errors.Trace(err))
		return
	}
	glog.Infof("JSON format: %s", js)
}
