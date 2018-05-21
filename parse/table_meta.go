package parse

import (
	"strings"
	"fmt"
	"github.com/juju/errors"
	"github.com/golang/glog"
	"sync"
	"encoding/json"
	"github.com/wingerx/drt/protoc"
	"github.com/wingerx/drt/parse/tsdb"
	"strconv"
)

const (
	TYPE_NUMBER    = iota + 1 // tinyint, smallint, mediumint, int, bigint, year
	TYPE_FLOAT                // float, double
	TYPE_ENUM                 // enum
	TYPE_SET                  // set
	TYPE_STRING               // other
	TYPE_DATETIME             // datetime
	TYPE_TIMESTAMP            // timestamp
	TYPE_DATE                 // date
	TYPE_TIME                 // time
	TYPE_BIT                  // bit
	TYPE_JSON                 // json
)

type TableMetaCache struct {
	conn        *MySQLConnection
	destination string

	lock  sync.Mutex
	cache *MemoryTableMeta
}

func NewTableMetaCache(conn *MySQLConnection, destination string) (*TableMetaCache, error) {
	tmc := new(TableMetaCache)
	tmc.conn = conn
	tmc.destination = destination
	if err := tmc.initAllTableMetaViaDB(); err != nil {
		return nil, err
	}

	return tmc, nil
}

func (tmc *TableMetaCache) RemoveOneTableMeta(schema string, table string) {
	if tmc.cache == nil || len(tmc.cache.tableMetas) <= 0 {
		return
	}

	if len(table) > 0 {
		tmc.lock.Lock()
		delete(tmc.cache.tableMetas, fmt.Sprintf("%s.%s", schema, table))
		tmc.lock.Unlock()
		return
	}
	// 依据schema 删除
	var fullNames []string
	for key := range tmc.cache.tableMetas {
		if schema == strings.Split(key, ".")[0] {
			fullNames = append(fullNames, key)
		}
	}

	if len(fullNames) > 0 {
		tmc.lock.Lock()
		for _, name := range fullNames {
			delete(tmc.cache.tableMetas, name)
		}
		tmc.lock.Unlock()
	}

}

func (tmc *TableMetaCache) RestoreOneTableMeta(schema string, table string) error {
	if len(table) > 0 {
		if exist, err := tmc.checkTableExistViaDB(schema, table); err != nil {
			return err
		} else if exist {
			return tmc.restoreOneTableMeta(schema, table)
		}
		return nil
	}
	return tmc.restoreOneTableMeta(schema, table)
}

func (tmc *TableMetaCache) GetOneTableMeta(schema string, table string) *TableMeta {
	tmc.lock.Lock()
	defer tmc.lock.Unlock()
	return tmc.cache.tableMetas[fmt.Sprintf("%s.%s", schema, table)]
}

func (tmc *TableMetaCache) GetOneTableMetaViaTSDB(schema, table string, position *protoc.EntryPosition, useCache bool) *TableMeta {
	// cache -> tsdb -> 源库
	var tm *TableMeta
	if useCache {
		tm = tmc.GetOneTableMeta(schema, table)
	}

	if tm == nil {
		// 通过 position 从 TSDB 数据库中获取与变更点最近的上一次表结构信息
		mh, err := tsdb.SelectLatestTableMeta(tmc.destination, schema, table, position.Timestamp)
		if err == nil && mh != nil {
			var t TableMeta
			err = json.Unmarshal([]byte(mh.TableMetaValue), &t)
			if err != nil {
				errLog.Print(errors.Trace(err))
			} else {
				tm = &t
			}
		}

		if tm == nil {
			// 从源端库重新获取
			err = tmc.RestoreOneTableMeta(schema, table)
			if err != nil {
				glog.Error(errors.Trace(err))
			}
			tm = tmc.GetOneTableMeta(schema, table)
			// 将该表结构持久化到tsdb, 作为新的基准线
			go func() {
				if tm != nil {
					tableMetaValue, err := json.Marshal(tm)
					if err != nil {
						glog.Error(errors.Trace(err))
						return
					}
					meta := new(tsdb.MetaHistory)
					meta.DdlType = "INIT"
					meta.SchemaName = schema
					meta.TableName = table
					meta.TableMetaValue = string(tableMetaValue)
					meta.Destination = tmc.destination
					meta.LogfileName = position.LogfileName
					meta.LogPosition = position.LogPosition
					meta.ExecuteTime = position.Timestamp
					meta.ServerId = strconv.FormatInt(position.ServerId, 10)
					// 插入数据库
					tsdb.InsertTableMeta(meta)
				}
			}()
		}
	}
	return tm
}

func (tmc *TableMetaCache) checkTableExistViaDB(schema string, name string) (bool, error) {
	query := fmt.Sprintf("SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '%s' and TABLE_NAME = '%s' LIMIT 1", schema, name)
	r, err := tmc.conn.Query(query)
	if err != nil {
		return false, errors.Trace(err)
	}

	return r.RowNumber() == 1, nil
}

func (tmc *TableMetaCache) restoreOneTableMeta(schema string, table string) error {
	cols, err := tmc.fetchTableColumnsViaDB(schema, table)
	if err != nil {
		return err
	}
	idx, err := tmc.fetchTableIndexesViaDB(schema, table)
	if err != nil {
		return err
	}

	tmc.lock.Lock()
	delete(tmc.cache.tableMetas, fmt.Sprintf("%s.%s", schema, table))
	for _, val := range cols {
		tmc.cache.addTableMetaColumn(schema, table, val)
	}
	for _, val := range idx {
		tmc.cache.addTableMetaIndex(schema, table, val)
	}
	tmc.lock.Unlock()
	return nil
}

func (tmc *TableMetaCache) restoreTableMetas(schema string) error {
	// 通过schema 查询所有table
	q := fmt.Sprintf("show tables from `%s`", schema)
	r, _ := tmc.conn.Query(q)
	var tables []string
	for i := range r.Values {
		tn, _ := r.GetString(i, 0)
		tables = append(tables, tn)
	}
	for _, t := range tables {
		tmc.restoreOneTableMeta(schema, t)
	}
	return nil
}

func (tmc *TableMetaCache) initAllTableMetaViaDB() error {
	q := "SELECT distinct table_schema FROM information_schema.tables where table_schema not in ('information_schema', 'mysql', 'performance_schema', 'sys')"
	r, err := tmc.conn.Query(q)
	if err != nil {
		glog.Errorf("fetch schema err: %s", err.Error())
		return err
	}

	schemas := make([]string, len(r.Values))
	for i := range r.Values {
		schemas[i], _ = r.GetString(i, 0)
	}

	var fullNameTables []string
	for _, s := range schemas {
		q := fmt.Sprintf("show tables from `%s`", s)
		r, _ := tmc.conn.Query(q)
		var tables []string
		for i := range r.Values {
			tn, _ := r.GetString(i, 0)
			tables = append(tables, tn)
		}

		for _, t := range tables {
			fullNameTables = append(fullNameTables, fmt.Sprintf("%s.%s", s, t))
		}
	}

	tblMap := make(map[string][]*RawTableColumn)
	idxMap := make(map[string][]*RawIndex)
	if len(fullNameTables) > 0 {
		for _, fullName := range fullNameTables {
			names := strings.Split(fullName, ".")
			cols, err := tmc.fetchTableColumnsViaDB(names[0], names[1])
			if err != nil {
				return err
			}
			if len(cols) > 0 {
				tblMap[fullName] = cols
			}

			// index
			idx, err := tmc.fetchTableIndexesViaDB(names[0], names[1])
			if err != nil {
				return err
			}
			if len(idx) > 0 {
				idxMap[fullName] = idx
			}
		}
	}

	mtm := &MemoryTableMeta{}
	mtm.tableMetas = make(map[string]*TableMeta)

	if len(tblMap) > 0 {
		for fullName, value := range tblMap {
			names := strings.Split(fullName, ".")
			for _, val := range value {
				mtm.addTableMetaColumn(names[0], names[1], val)
			}
		}
	}

	if len(idxMap) > 0 {
		for fullName, value := range idxMap {
			names := strings.Split(fullName, ".")
			for _, val := range value {
				mtm.addTableMetaIndex(names[0], names[1], val)
			}
		}
	}

	tmc.lock.Lock()
	defer tmc.lock.Unlock()
	tmc.cache = mtm

	return nil
}

func (tmc *TableMetaCache) fetchTableColumnsViaDB(schema string, table string) ([]*RawTableColumn, error) {
	q := fmt.Sprintf("show full columns from `%s`.`%s`;", schema, table)
	r, err := tmc.conn.Query(q)
	if err != nil {
		glog.Errorf("fetch table column err: %s", err.Error())
		return nil, err
	}

	tblColumns := make([]*RawTableColumn, len(r.Values))
	for i := range r.Values {
		// field, type, collation, null, key, default, extra
		rtc := new(RawTableColumn)
		rtc.name, _ = r.GetString(i, 0)
		rtc.rawType, _ = r.GetString(i, 1)
		rtc.collation, _ = r.GetString(i, 2)
		rtc.null, _ = r.GetString(i, 3)
		rtc.extra, _ = r.GetString(i, 6)
		tblColumns[i] = rtc
	}

	return tblColumns, nil
}

func (tmc *TableMetaCache) fetchTableIndexesViaDB(schema string, table string) ([]*RawIndex, error) {
	q := fmt.Sprintf("show index from `%s`.`%s`;", schema, table)
	r, err := tmc.conn.Query(q)
	if err != nil {
		glog.Errorf("fetch table index err: %s", err.Error())
		return nil, err
	}

	tblIndexes := make([]*RawIndex, len(r.Values))
	for i := range r.Values {
		// table, non_unique, key_name, seq_in_index, column_name, collation, extra
		rawIndex := new(RawIndex)
		rawIndex.name, _ = r.GetString(i, 2)
		rawIndex.column, _ = r.GetString(i, 4)

		if rawIndex.name == "PRIMARY" {
			rawIndex.isPK = true
		}
		uk, _ := r.GetInt64(i, 1)
		if uk == 0 {
			rawIndex.isUnique = true
		}
		tblIndexes[i] = rawIndex
	}

	return tblIndexes, nil
}

type MemoryTableMeta struct {
	tableMetas map[string]*TableMeta
}

func (mt *MemoryTableMeta) findTableMeta(schema string, table string) (*TableMeta, error) {
	fullName := fmt.Sprintf("%s.%s", schema, table)
	if tm, ok := mt.tableMetas[fullName]; ok {
		return tm, nil
	}
	return nil, errors.New(fmt.Sprintf("%s not found", fullName))
}

func (mt *MemoryTableMeta) addTableMetaColumn(schema string, table string, column *RawTableColumn) {
	tm, _ := mt.findTableMeta(schema, table)
	if tm == nil {
		tm = &TableMeta{
			Schema: schema,
			Table:  table,
		}
	}
	if column != nil {
		tm.addTableColumn(column.name, column.rawType, column.collation, column.null, column.extra)
	}
	fullName := fmt.Sprintf("%s.%s", schema, table)
	mt.tableMetas[fullName] = tm
}

func (mt *MemoryTableMeta) addTableMetaIndex(schema string, table string, index *RawIndex) {
	tm, _ := mt.findTableMeta(schema, table)
	if tm == nil {
		tm = &TableMeta{
			Schema: schema,
			Table:  table,
		}
	}
	if index != nil {
		tm.addIndex(index.name, index.column, index.isPK, index.isUnique)
	}
	fullName := fmt.Sprintf("%s.%s", schema, table)
	mt.tableMetas[fullName] = tm
}

func (mt *MemoryTableMeta) size() int {
	return len(mt.tableMetas)
}

type RawTableColumn struct {
	name      string
	rawType   string
	collation string
	null      string
	extra     string
}

type RawIndex struct {
	name     string
	column   string
	isPK     bool
	isUnique bool
}

type TableMeta struct {
	Schema string
	Table  string

	Columns []*TableColumn
	Indexes map[string]*Index
}

type TableColumn struct {
	Name       string
	Type       int
	RawType    string
	Collation  string
	Nullable   bool
	IsAuto     bool
	IsUnsigned bool
	EnumValues []string
	SetValues  []string
}

type Index struct {
	Name     string
	Columns  []string
	IsPK     bool
	IsUnique bool
}

func (tbl *TableMeta) addTableColumn(name string, columnType string, collation string, null string, extra string) {
	index := len(tbl.Columns)
	tbl.Columns = append(tbl.Columns, &TableColumn{Name: name, Collation: collation})

	tbl.Columns[index].RawType = columnType
	if strings.HasPrefix(columnType, "float") ||
		strings.HasPrefix(columnType, "double") ||
		strings.HasPrefix(columnType, "decimal") {
		tbl.Columns[index].Type = TYPE_FLOAT
	} else if strings.HasPrefix(columnType, "enum") {
		tbl.Columns[index].Type = TYPE_ENUM
		tbl.Columns[index].EnumValues = strings.Split(strings.Replace(
			strings.TrimSuffix(
				strings.TrimPrefix(
					columnType, "enum("),
				")"),
			"'", "", -1),
			",")
	} else if strings.HasPrefix(columnType, "set") {
		tbl.Columns[index].Type = TYPE_SET
		tbl.Columns[index].SetValues = strings.Split(strings.Replace(
			strings.TrimSuffix(
				strings.TrimPrefix(
					columnType, "set("),
				")"),
			"'", "", -1),
			",")
	} else if strings.HasPrefix(columnType, "datetime") {
		tbl.Columns[index].Type = TYPE_DATETIME
	} else if strings.HasPrefix(columnType, "timestamp") {
		tbl.Columns[index].Type = TYPE_TIMESTAMP
	} else if strings.HasPrefix(columnType, "time") {
		tbl.Columns[index].Type = TYPE_TIME
	} else if "date" == columnType {
		tbl.Columns[index].Type = TYPE_DATE
	} else if strings.HasPrefix(columnType, "bit") {
		tbl.Columns[index].Type = TYPE_BIT
	} else if strings.HasPrefix(columnType, "json") {
		tbl.Columns[index].Type = TYPE_JSON
	} else if strings.Contains(columnType, "int") { //|| strings.HasPrefix(columnType, "year")
		tbl.Columns[index].Type = TYPE_NUMBER
	} else {
		tbl.Columns[index].Type = TYPE_STRING
	}

	if strings.Contains(columnType, "unsigned") || strings.Contains(columnType, "zerofill") {
		tbl.Columns[index].IsUnsigned = true
	}

	if extra == "auto_increment" {
		tbl.Columns[index].IsAuto = true
	}

	if null == "YES" {
		tbl.Columns[index].Nullable = true
	}
}

func (tbl *TableMeta) addIndex(idxName string, idxColName string, isPK bool, isUnique bool) {
	if tbl.Indexes == nil {
		tbl.Indexes = make(map[string]*Index)
	}
	if idx, ok := tbl.Indexes[idxName]; ok {
		idx.Columns = append(idx.Columns, idxColName)
		idx.IsPK = isPK
		idx.IsUnique = isUnique
		tbl.Indexes[idxName] = idx
	} else {
		idx := &Index{Name: idxName, Columns: make([]string, 0, 8)}
		idx.Columns = append(idx.Columns, idxColName)
		idx.IsPK = isPK
		idx.IsUnique = isUnique
		tbl.Indexes[idxName] = idx
	}
}

func (tbl *TableMeta) findTableColumn(i int) *TableColumn {
	if i > len(tbl.Columns) {
		return nil
	}
	return tbl.Columns[i]
}

func (tbl *TableMeta) isPKColumn(colName string) bool {
	if len(colName) <= 0 || len(tbl.Indexes) <= 0 {
		return false
	}
	for _, val := range tbl.Indexes {
		if len(val.Columns) <= 0 {
			continue
		}
		for _, v := range val.Columns {
			if strings.EqualFold(colName, v) {
				return true
			}
		}
	}
	return false
}

type TableMetaTSDB struct {
	destination string

	lock  sync.RWMutex
	cache *MemoryTableMeta
}

func (tmTSDB *TableMetaTSDB) initAllTableMetaViaTSDB(destination string) error {
	tmTSDB.destination = destination
	return nil
}
