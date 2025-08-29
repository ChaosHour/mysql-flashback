package models

import "fmt"

type DBTable struct {
	TableSchema string `gorm:"column:TABLE_SCHEMA"`
	TableName   string `gorm:"column:TABLE_NAME"`
}

func (d *DBTable) String() string {
	return fmt.Sprintf("%s.%s", d.TableSchema, d.TableName)
}

func NewDBTable(schema string, table string) *DBTable {
	return &DBTable{
		TableSchema: schema,
		TableName:   table,
	}
}
