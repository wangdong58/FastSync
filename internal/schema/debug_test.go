package schema

import (
	"database/sql"
	"fmt"
	"testing"
)

func TestDebugValidation(t *testing.T) {
	// 连接SQL Server
	srcDB, err := sql.Open("sqlserver", "sqlserver://cdctest:Hello@1234@10.106.1.101:1433?database=JYPRIME")
	if err != nil {
		t.Fatal(err)
	}
	defer srcDB.Close()
	
	// 获取源端结构
	sd := NewSchemaDiscovery(srcDB)
	srcSchema, err := sd.DiscoverExtendedTable("dbo", "usrZQSYL")
	if err != nil {
		t.Fatal(err)
	}
	
	fmt.Printf("Source columns count: %d\n", len(srcSchema.Columns))
	for _, col := range srcSchema.Columns {
		fmt.Printf("  Source: '%s'\n", col.Name)
	}
}
