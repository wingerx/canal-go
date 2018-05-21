package tsdb

import (
	"time"
	"github.com/wingerx/drt/protoc"
	"github.com/golang/glog"
	"fmt"
	"regexp"
)

type MetaHistory struct {
	Id             int64     `json:"id"`
	GmtCreate      time.Time `json:"gmtCreate"`
	GmtModified    time.Time `json:"gmtModified"`
	Destination    string    `json:"destination"`
	SchemaName     string    `json:"schemaName"`
	TableName      string    `json:"tableName"`
	LogfileName    string    `json:"logfileName"`
	LogPosition    int64     `json:"logPosition"`
	ServerId       string    `json:"serverId"`
	ExecuteTime    int64     `json:"executeTime"`
	TableMetaValue string    `json:"tableMetaValue"`

	SqlText string `json:"sqlText"`
	DdlType string `json:"ddlType"`
}

var pattern, _ = regexp.Compile("Duplicate entry '.*' for key '*'")

func SelectLatestTableMeta(destination, schema, table string, executeTime int64) (*MetaHistory, error) {
	sql := "select * from drt_tsdb.meta_history where destination = ? and executeTime < ? and schemaName = ? and tableName = ? order by executeTime desc,id desc limit 1"
	var mh MetaHistory
	err := db.Get(&mh, sql, destination, executeTime, schema, table)
	if err != nil {
		errLog.Print(fmt.Sprintf("get %s.%s with %d has error: %s", schema, table, executeTime, err.Error()))
	}
	return &mh, err
}

func InsertTableMeta(mh *MetaHistory) {
	sql := "insert into drt_tsdb.meta_history(gmtCreate, gmtModified, destination, schemaName, tableName, logfileName, logPosition, serverId, executeTime, tableMetaValue, sqlText, ddlType) values (now(),now(),?,?,?,?,?,?,?,?,?,?)"
	_, err := db.Exec(sql, mh.Destination, mh.SchemaName, mh.TableName, mh.LogfileName, mh.LogPosition, mh.ServerId, mh.ExecuteTime, mh.TableMetaValue, mh.SqlText, mh.DdlType)
	if err != nil && !pattern.MatchString(err.Error()) {
		errLog.Print(err)
	}
}

func DeleteTableMetaViaDestination(destination string) {
	sql := "delete from drt_tsdb.meta_history where destination=?"
	_, err := db.Exec(sql, destination)
	if err != nil {
		glog.Error(err)
	}
}

func DeleteTableMeta(destination string, position *protoc.EntryPosition) {
	sql := "delete from drt_tsdb.meta_history where destination=? and logfileName =? and logPosition =? and serverId =?"
	_, err := db.Exec(sql, destination, position.LogfileName, position.LogPosition, position.ServerId)
	if err != nil {
		glog.Error(err)
	}
}

var createTableStatements = []string{
	`CREATE DATABASE IF NOT EXISTS drt_tsdb DEFAULT CHARACTER SET = 'utf8' DEFAULT COLLATE 'utf8_general_ci';`,
	`USE drt_tsdb;`,
	"CREATE TABLE IF NOT EXISTS `meta_history` (" +
		"`id` bigint(20) unsigned NOT NULL AUTO_INCREMENT COMMENT '主键'," +
		"`gmtCreate` datetime NOT NULL COMMENT '创建时间',  " +
		"`gmtModified` datetime NOT NULL COMMENT '修改时间'," +
		"`destination` varchar(128) DEFAULT NULL COMMENT '通道名称'," +
		"`schemaName` varchar(128) DEFAULT NULL COMMENT 'schema 名称'," +
		"`tableName` varchar(128) DEFAULT NULL COMMENT 'table 名称'," +
		"`logfileName` varchar(64) DEFAULT NULL COMMENT 'binlog文件名'," +
		"`logPosition` bigint(20) DEFAULT NULL COMMENT 'binlog偏移量'," +
		"`serverId` varchar(64) DEFAULT NULL COMMENT 'binlog节点id'," +
		"`executeTime` bigint(20) DEFAULT NULL COMMENT 'binlog应用的时间戳'," +
		"`tableMetaValue` longtext DEFAULT NULL COMMENT '对应的table meta信息'," +
		"`sqlText` longtext DEFAULT NULL COMMENT '执行变更时的sql 文本'," +
		"`ddlType` varchar(64) DEFAULT NULL COMMENT '执行变更时的 sql 类型'," +
		"PRIMARY KEY (`id`),  " +
		"	UNIQUE KEY binlog_file_pos(`destination`,`serverId`,`logfileName`,`logPosition`)," +
		"	KEY `destination` (`destination`)," +
		"	KEY `destination_timestamp` (`destination`,`executeTime`,`schemaName`,`tableName`)," +
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
