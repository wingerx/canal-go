package tsdb

import (
	"time"
	"github.com/wingerx/drt/protoc"
	"github.com/golang/glog"
)

type MetaHistory struct {
	Id             int64      `json:id`
	GmtCreate      *time.Time `json:gmtCreate`
	GmtModified    *time.Time `json:gmtModified`
	Destination    string     `json:destination`
	LogfileName    string     `json:logfileName`
	LogPosition    int64      `json:logPosition`
	ServerId       string     `json:serverId`
	ExecuteTime    int64      `json:executeTime`
	TableMetaValue string     `json:tableMetaValue`

	SqlText string `json:sqlText`
	DdlType string `json:ddlType`
}

func SelectLastestTableMeta(destination string, executeTime int64) (mh *MetaHistory, err error) {
	sql := "select * from meta_history where destination = ? and executeTime <= ? order by executeTime desc,id desc limit 1"
	db.Get(&mh, sql, destination, executeTime)
	return
}
func SelectTableMeta(destination string, position *protoc.EntryPosition) (mh *MetaHistory, err error) {
	sql := "select * from meta_history where destination = ? and logfileName = ? and logPosition = ? and serverId = ? and executeTime = ?"
	db.Get(&mh, sql, destination, position.LogfileName, position.LogPosition, position.ServerId, position.Timestamp)
	return
}

func InsertTableMeta(mh *MetaHistory) {
	sql := "replace into meta_history(gmtCreate, gmtModified, destination, logfileName, logPosition, serverId, executeTime, tableMeta, sqlText, DdlType) values (now(),now(),?,?,?,?,?,?,?,?)"
	db.MustExec(sql, mh.Destination, mh.LogfileName, mh.LogPosition, mh.ServerId, mh.ExecuteTime, mh.TableMetaValue, mh.SqlText, mh.DdlType)
}

func DeleteTableMetaViaDestination(destination string) {
	sql := "delete from meta_history where destination=?"
	db.MustExec(sql, destination)
}

func DeleteTableMeta(destination string, position *protoc.EntryPosition) {
	sql := "delete from meta_history where destination=? and logfileName = ? and logPosition = ? and serverId = ?"
	db.MustExec(sql, destination)
}

var createTableStatements = []string{
	`CREATE DATABASE IF NOT EXISTS drt_tsdb DEFAULT CHARACTER SET = 'utf8' DEFAULT COLLATE 'utf8_general_ci';`,
	`USE drt_tsdb;`,
	"CREATE TABLE IF NOT EXISTS `meta_history` (" +
		"`id` bigint(20) unsigned NOT NULL AUTO_INCREMENT COMMENT '主键'," +
		"`gmtCreate` datetime NOT NULL COMMENT '创建时间',  " +
		"`gmtModified` datetime NOT NULL COMMENT '修改时间'," +
		"`destination` varchar(128) DEFAULT NULL COMMENT '通道名称'," +
		"`logfileName` varchar(64) DEFAULT NULL COMMENT 'binlog文件名'," +
		"`logPosition` bigint(20) DEFAULT NULL COMMENT 'binlog偏移量'," +
		"`serverId` varchar(64) DEFAULT NULL COMMENT 'binlog节点id'," +
		"`executeTime` bigint(20) DEFAULT NULL COMMENT 'binlog应用的时间戳'," +
		"`tableMetaValue` varchar(1024) DEFAULT NULL COMMENT '对应的table meta信息'," +
		"`sqlText` longtext DEFAULT NULL COMMENT '执行变更时的sql 文本'," +
		"`ddlType` varchar(1024) DEFAULT NULL COMMENT '执行变更时的 sql 类型'," +
		"PRIMARY KEY (`id`),  " +
		"	UNIQUE KEY binlog_file_pos(`destination`,`serverId`,`logfileName`,`logPosition`)," +
		"	KEY `destination` (`destination`)," +
		"	KEY `destination_timestamp` (`destination`,`executeTime`)," +
		"	KEY `gmt_modified` (`gmtModified`)" +
		") ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8 COMMENT='表结构变化历史表';"}

func InitTableMeta() {
	for _, stmt := range createTableStatements {
		if _, err := db.Exec(stmt); err != nil {
			glog.Fatal(err)
		}
	}
}

type DatabaseTableMeta struct {
}
