package parse

import (
	"github.com/golang/glog"
	"github.com/knocknote/vitess-sqlparser/tidbparser/ast"
	"github.com/knocknote/vitess-sqlparser/tidbparser/parser"
	. "github.com/woqutech/drt/protoc"
)

type DdlParserResult struct {
	schemaName    string
	tableName     string
	oriSchemaName string
	oriTableName  string
	eventType     EventType
}

type DdlSQLParser struct {
	sql string
}

func NewSQLParser(sql string) *DdlSQLParser {
	return &DdlSQLParser{
		sql: sql,
	}
}
func (dsp *DdlSQLParser) Parse(dbName string) ([]*DdlParserResult, error) {
	p := parser.New()
	p.Parse(dsp.sql, "", "")
	stmtNodes, err := p.Parse(dsp.sql, "", "")
	var ddlResults []*DdlParserResult

	if err != nil {
		glog.Errorf("parse sql [%v] error: %v", dsp.sql, err.Error())

		sqlResult := new(DdlParserResult)
		sqlResult.eventType = EventType_QUERY
		sqlResult.oriSchemaName = dbName
		return append(ddlResults, sqlResult), err
	}

	for _, stmt := range stmtNodes {
		switch node := stmt.(type) {
		case *ast.CreateTableStmt:
			sqlResult := new(DdlParserResult)
			sqlResult.eventType = EventType_CREATE
			sqlResult.schemaName = node.Table.Schema.String()
			sqlResult.tableName = node.Table.Name.String()
			if len(sqlResult.schemaName) <= 0 {
				sqlResult.schemaName = dbName
			}
			ddlResults = append(ddlResults, sqlResult)
		case *ast.AlterTableStmt:
			if len(node.Specs) > 0 {
				for _, spec := range node.Specs {
					switch spec.Tp {
					case ast.AlterTableRenameTable:
						sqlResult := new(DdlParserResult)
						sqlResult.eventType = EventType_RENAME
						sqlResult.oriSchemaName = node.Table.Schema.String()
						sqlResult.oriTableName = node.Table.Name.String()
						sqlResult.schemaName = spec.NewTable.Schema.String()
						sqlResult.tableName = spec.NewTable.Name.String()

						if len(sqlResult.oriSchemaName) <= 0 {
							sqlResult.schemaName = dbName
							sqlResult.oriSchemaName = dbName
						}
						ddlResults = append(ddlResults, sqlResult)
					case ast.AlterTableAddConstraint:
						sqlResult := new(DdlParserResult)
						sqlResult.eventType = EventType_CINDEX
						sqlResult.schemaName = node.Table.Schema.String()
						sqlResult.tableName = node.Table.Name.String()
						if len(sqlResult.schemaName) <= 0 {
							sqlResult.schemaName = dbName
						}
						ddlResults = append(ddlResults, sqlResult)
					case ast.AlterTableDropIndex, ast.AlterTableDropForeignKey, ast.AlterTableDropPrimaryKey:
						sqlResult := new(DdlParserResult)
						sqlResult.eventType = EventType_DINDEX
						sqlResult.schemaName = node.Table.Schema.String()
						sqlResult.tableName = node.Table.Name.String()
						if len(sqlResult.schemaName) <= 0 {
							sqlResult.schemaName = dbName
						}
						ddlResults = append(ddlResults, sqlResult)
					default:
						sqlResult := new(DdlParserResult)
						sqlResult.eventType = EventType_ALTER
						sqlResult.schemaName = node.Table.Schema.String()
						sqlResult.tableName = node.Table.Name.String()
						if len(sqlResult.schemaName) <= 0 {
							sqlResult.schemaName = dbName
						}
						ddlResults = append(ddlResults, sqlResult)
					}
				}
			}
		case *ast.DropTableStmt:
			for _, t := range node.Tables {
				sqlResult := new(DdlParserResult)
				sqlResult.eventType = EventType_DROP
				sqlResult.schemaName = t.Schema.String()
				sqlResult.tableName = t.Name.String()
				if len(sqlResult.schemaName) <= 0 {
					sqlResult.schemaName = dbName
				}
				ddlResults = append(ddlResults, sqlResult)
			}
		case *ast.CreateIndexStmt:
			sqlResult := new(DdlParserResult)
			sqlResult.eventType = EventType_CINDEX
			sqlResult.schemaName = node.Table.Schema.String()
			sqlResult.tableName = node.Table.Name.String()
			if len(sqlResult.schemaName) <= 0 {
				sqlResult.schemaName = dbName
			}
			ddlResults = append(ddlResults, sqlResult)
		case *ast.DropIndexStmt:
			sqlResult := new(DdlParserResult)
			sqlResult.eventType = EventType_DINDEX
			sqlResult.schemaName = node.Table.Schema.String()
			sqlResult.tableName = node.Table.Name.String()
			if len(sqlResult.schemaName) <= 0 {
				sqlResult.schemaName = dbName
			}
			ddlResults = append(ddlResults, sqlResult)
		case *ast.TruncateTableStmt:
			sqlResult := new(DdlParserResult)
			sqlResult.eventType = EventType_TRUNCATE
			sqlResult.schemaName = node.Table.Schema.String()
			sqlResult.tableName = node.Table.Name.String()
			if len(sqlResult.schemaName) <= 0 {
				sqlResult.schemaName = dbName
			}
			ddlResults = append(ddlResults, sqlResult)
		case *ast.RenameTableStmt:
			sqlResult := new(DdlParserResult)
			sqlResult.eventType = EventType_RENAME
			sqlResult.oriSchemaName = node.OldTable.Schema.String()
			sqlResult.oriTableName = node.OldTable.Name.String()
			sqlResult.schemaName = node.NewTable.Schema.String()
			sqlResult.tableName = node.NewTable.Name.String()
			if len(sqlResult.oriSchemaName) <= 0 {
				sqlResult.schemaName = dbName
				sqlResult.oriSchemaName = dbName
			}
			ddlResults = append(ddlResults, sqlResult)
		case *ast.InsertStmt:
			sqlResult := new(DdlParserResult)
			sqlResult.eventType = EventType_INSERT
			switch rsn := node.Table.TableRefs.Left.(type) {
			case *ast.TableSource:
				switch tbl := rsn.Source.(type) {
				case *ast.TableName:
					sqlResult.schemaName = tbl.Schema.String()
					sqlResult.tableName = tbl.Name.String()
				}
			}

			if len(sqlResult.schemaName) <= 0 {
				sqlResult.schemaName = dbName
			}
			ddlResults = append(ddlResults, sqlResult)
		case *ast.UpdateStmt:
			sqlResult := new(DdlParserResult)
			sqlResult.eventType = EventType_UPDATE
			switch rsn := node.TableRefs.TableRefs.Left.(type) {
			case *ast.TableSource:
				switch tbl := rsn.Source.(type) {
				case *ast.TableName:
					sqlResult.schemaName = tbl.Schema.String()
					sqlResult.tableName = tbl.Name.String()
				}
			}

			if len(sqlResult.schemaName) <= 0 {
				sqlResult.schemaName = dbName
			}
			ddlResults = append(ddlResults, sqlResult)
		case *ast.DeleteStmt:
			sqlResult := new(DdlParserResult)
			sqlResult.eventType = EventType_DELETE
			switch rsn := node.TableRefs.TableRefs.Left.(type) {
			case *ast.TableSource:
				switch tbl := rsn.Source.(type) {
				case *ast.TableName:
					sqlResult.schemaName = tbl.Schema.String()
					sqlResult.tableName = tbl.Name.String()
				}
			}
			if len(sqlResult.schemaName) <= 0 {
				sqlResult.schemaName = dbName
			}
			ddlResults = append(ddlResults, sqlResult)
		}
	}

	return ddlResults, nil
}

func (dsp *DdlSQLParser) processName(dbName string) {

}
