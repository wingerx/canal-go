package tsdb

import (
	"github.com/jmoiron/sqlx"
	"sync"
	"fmt"
	"github.com/jmoiron/sqlx/reflectx"
	"strings"
	"github.com/golang/glog"

	_ "github.com/go-sql-driver/mysql"
)

type Info struct {
	Address  string
	Port     string
	Username string
	Password string
	Database string
}

var db *sqlx.DB
var once sync.Once

func NewOnceConn(info *Info) *sqlx.DB {
	once.Do(func() {
		glog.Infof("init db connection with %#v\n", info)
		_db, err := sqlx.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?charset=utf8&parseTime=true", info.Username, info.Password, info.Address, info.Port, info.Database))
		if err != nil {
			glog.Fatal(err)
		}
		// Create a new mapper which will use the struct field tag "json" instead of "db"
		_db.Mapper = reflectx.NewMapperFunc("json", strings.ToLower)
		db = _db
	})
	return db
}
