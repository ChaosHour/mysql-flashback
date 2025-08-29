package offline_stat

import "fmt"

type DmlStat struct {
	InsertCount int `json:"insert_count"`
	UpdateCount int `json:"update_count"`
	DeleteCount int `json:"delete_count"`
}

type BinlogFile struct {
	StartLogFile string `json:"start_log_file"`
	StartLogPos  uint32 `json:"start_log_pos"`
}

func (b *BinlogFile) FilePos() string {
	return fmt.Sprintf("%v:%v", b.StartLogFile, b.StartLogPos)
}

func (d *DmlStat) DmlCount() int {
	return d.InsertCount + d.UpdateCount + d.DeleteCount
}

type TableBinlogStat struct {
	DmlStat
	SchemaName  string `json:"schema_name"`
	TableName   string `json:"table_name"`
	AppearCount int64  `json:"appear_count"`
}
