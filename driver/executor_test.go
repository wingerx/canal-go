package driver

import (
	"testing"
	"context"
	"fmt"
	"bytes"
)

func TestExecutor_Query(t *testing.T) {
	// 正常连接，无默认db
	ctx := context.Background()
	mc := NewMySQLConnector("127.0.0.1:3306", "root", "123456", "example")
	err := mc.Connect(ctx)
	defer mc.Close()
	if err != nil {
		fmt.Println(err)
		return
	}
	query := "SELECT distinct table_schema FROM information_schema.tables where table_schema not in ('information_schema', 'mysql', 'performance_schema', 'sys')"
	result, err := mc.Query(query)
	if err != nil {
		mc.Close()
		fmt.Println(err)
		return
	}
	schemas := make([]string, len(result.Values))

	for idx := range result.Values {
		schemas[idx], _ = result.GetString(idx, 0)
	}

	var buffer bytes.Buffer

	for _, s := range schemas {
		q := fmt.Sprintf("show tables from `%v`", s)
		r, _ := mc.Query(q)
		var tables []string
		for i := range r.Values {
			tn, _ := r.GetString(i, 0)
			tables = append(tables, tn)
		}

		for _, t := range tables {
			buffer.WriteString(fmt.Sprintf("show create table `%v`.`%v`;", s, t))
		}
	}

	query = buffer.String()
	multi, err := mc.QueryMulti(query)

	if err != nil {
		mc.Close()
		fmt.Println(err)
		return
	}

	for _, items := range multi {
		for idx := range items.Values {
			fmt.Println(items.GetString(idx, 1))
		}
	}

	//for idx := range result.Values {
	//	fmt.Println(result.GetString(idx, 0))
	//}
}
