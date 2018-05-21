package tsdb

import (
	"github.com/jmoiron/sqlx"
	"sync"
	"fmt"
	"github.com/golang/glog"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx/reflectx"
	"strings"
	"os"
	"log"
	"github.com/wingerx/drt/tools"
)

type MysqlConfig struct {
	Address  string
	Port     string
	Username string
	Password string
	Database string
}

var db *sqlx.DB
var once sync.Once

func NewOnceConn(config *MysqlConfig) *sqlx.DB {
	once.Do(func() {
		glog.Infof("init db connection with %#v\n", config)
		_db := sqlx.MustOpen("mysql", fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?charset=utf8&parseTime=true", config.Username, config.Password, config.Address, config.Port, config.Database))

		// Create a new mapper which will use the struct field tag "json" instead of "db"
		_db.Mapper = reflectx.NewMapperFunc("json", strings.ToLower)
		db = _db
	})
	return db
}

var errLog tools.Logger = log.New(os.Stderr, "[TSDB] ", log.Ldate|log.Ltime|log.Lshortfile)
