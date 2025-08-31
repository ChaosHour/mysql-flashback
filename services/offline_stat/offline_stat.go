package offline_stat

import (
	"fmt"
	"os"
	"sort"

	"github.com/ChaosHour/mysql-flashback/config"
	"github.com/ChaosHour/mysql-flashback/utils"
	"github.com/cihub/seelog"
	"github.com/go-mysql-org/go-mysql/replication"
)

const (
	QueryEventBegin = "BEGIN"
)

type OfflineStat struct {
	OfflineStatCfg         *config.OfflineStatConfig
	TotalTableStatMap      map[string]*TableBinlogStat
	TotalThreadStatMap     map[uint32]*ThreadBinlogStat
	TotalTransactionStats  []*TransactionBinlogStat
	TotalTimestampStats    []*TimestampBinlogStat
	CurrentTimestampStat   *TimestampBinlogStat
	CurrentTransactionStat *TransactionBinlogStat
	CurrentTreadId         uint32
	CurrentXid             uint64
	CurrentTimestamp       uint32
	CurrentSchemaName      string
	CurrentTableName       string
	CurrentLogFile         string
	CurrentLogPos          uint32
}

func NewOfflineStat(cfg *config.OfflineStatConfig) *OfflineStat {
	return &OfflineStat{
		OfflineStatCfg:        cfg,
		TotalTableStatMap:     make(map[string]*TableBinlogStat),
		TotalThreadStatMap:    make(map[uint32]*ThreadBinlogStat),
		TotalTransactionStats: make([]*TransactionBinlogStat, 0, 1000),
		TotalTimestampStats:   make([]*TimestampBinlogStat, 0, 1000),
	}
}

func (o *OfflineStat) Start() error {
	for i, binlogFile := range o.OfflineStatCfg.BinlogFiles {
		o.CurrentLogFile = binlogFile
		seelog.Infof("Starting to parse Binlog: %v/%v, binlog file: %v", i+i, len(o.OfflineStatCfg.BinlogFiles), binlogFile)

		// Create a BinlogParser object
		parser := replication.NewBinlogParser()
		if err := parser.ParseFile(binlogFile, 0, func(event *replication.BinlogEvent) error {
			return o.handleEvent(event)
		}); err != nil {
			return fmt.Errorf("Error parsing binlog. Progress: %v/%v, binlog file: %v. %v", i+1, len(o.OfflineStatCfg.BinlogFiles), binlogFile, err)
		}
	}

	// Output statistics to file
	o.statToFile()

	return nil
}

// 处理binlog事件
func (o *OfflineStat) handleEvent(ev *replication.BinlogEvent) error {
	switch e := ev.Event.(type) {
	case *replication.XIDEvent:
		o.handleXIDEvent(e)
	case *replication.QueryEvent:
		o.handleQueryEvent(e, ev)
	case *replication.TableMapEvent:
		o.handleTableMapEvent(e)
	case *replication.RowsEvent:
		o.handleRowEvent(e, ev)
	}

	return nil
}

func (o *OfflineStat) handleXIDEvent(e *replication.XIDEvent) {
	if o.CurrentTransactionStat == nil {
		return
	}

	// 添加事务统计
	o.CurrentTransactionStat.Xid = e.XID
	o.TotalTransactionStats = append(o.TotalTransactionStats, o.CurrentTransactionStat)

	o.CurrentTransactionStat = nil
}

func (o *OfflineStat) handleQueryEvent(e *replication.QueryEvent, ev *replication.BinlogEvent) {
	o.CurrentTreadId = e.SlaveProxyID

	// 遇到 BEGIN
	if QueryEventBegin == string(e.Query) {
		// 添加和初始化时间统计
		if ev.Header.Timestamp != o.CurrentTimestamp {
			o.CurrentTimestampStat = NewTimestampBinlogStat(ev.Header.Timestamp, o.CurrentLogFile, ev.Header.LogPos)
			// 添加时间统计
			if o.CurrentTimestampStat != nil {
				o.TotalTimestampStats = append(o.TotalTimestampStats, o.CurrentTimestampStat)
			}
		}
		o.CurrentTimestampStat.TxCount += 1

		// 初始化事务统计
		o.CurrentTransactionStat = NewTransactionBinlogStat(ev.Header.Timestamp, o.CurrentLogFile, ev.Header.LogPos)

		// 初始化 threadId
		threadStat, ok := o.TotalThreadStatMap[e.SlaveProxyID]
		if !ok {
			threadStat = &ThreadBinlogStat{
				ThreadId: e.SlaveProxyID,
			}
			o.TotalThreadStatMap[e.SlaveProxyID] = threadStat
		}
		threadStat.AppearCount += 1
	}
}

func (o *OfflineStat) handleTableMapEvent(e *replication.TableMapEvent) {
	o.CurrentSchemaName = string(e.Schema)
	o.CurrentTableName = string(e.Table)
	table := fmt.Sprintf("%v.%v", o.CurrentSchemaName, o.CurrentTableName)

	tableStat, ok := o.TotalTableStatMap[table]
	if !ok {
		tableStat = &TableBinlogStat{
			SchemaName: o.CurrentSchemaName,
			TableName:  o.CurrentTableName,
		}

		o.TotalTableStatMap[table] = tableStat
	}

	// 统计表出现次数
	tableStat.AppearCount += 1
}

func (o *OfflineStat) handleRowEvent(e *replication.RowsEvent, ev *replication.BinlogEvent) {
	table := fmt.Sprintf("%v.%v", o.CurrentSchemaName, o.CurrentTableName)

	switch ev.Header.EventType {
	case replication.WRITE_ROWS_EVENTv0, replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2:
		// 表统计
		tableStat, ok := o.TotalTableStatMap[table]
		if ok {
			tableStat.InsertCount += len(e.Rows)
		}

		// 时间统计
		if o.CurrentTimestampStat != nil {
			o.CurrentTimestampStat.InsertCount += len(e.Rows)
		}

		// 事务统计
		if o.CurrentTransactionStat != nil {
			o.CurrentTransactionStat.InsertCount += len(e.Rows)
		}

		// Thread 统计
		threadStat, ok := o.TotalThreadStatMap[o.CurrentTreadId]
		if ok {
			threadStat.InsertCount += len(e.Rows)
		}
	case replication.UPDATE_ROWS_EVENTv0, replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2:
		// 表统计
		tableStat, ok := o.TotalTableStatMap[table]
		if ok {
			tableStat.UpdateCount += len(e.Rows) / 2
		}

		// 时间统计
		if o.CurrentTimestampStat != nil {
			o.CurrentTimestampStat.UpdateCount += len(e.Rows) / 2
		}

		// 事务统计
		if o.CurrentTransactionStat != nil {
			o.CurrentTransactionStat.UpdateCount += len(e.Rows) / 2
		}

		// Thread 统计
		threadStat, ok := o.TotalThreadStatMap[o.CurrentTreadId]
		if ok {
			threadStat.UpdateCount += len(e.Rows) / 2
		}
	case replication.DELETE_ROWS_EVENTv0, replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2:
		// 表统计
		tableStat, ok := o.TotalTableStatMap[table]
		if ok {
			tableStat.DeleteCount += len(e.Rows)
		}

		// 时间统计
		if o.CurrentTimestampStat != nil {
			o.CurrentTimestampStat.DeleteCount += len(e.Rows)
		}

		// 事务统计
		if o.CurrentTransactionStat != nil {
			o.CurrentTransactionStat.DeleteCount += len(e.Rows)
		}

		// Thread 统计
		threadStat, ok := o.TotalThreadStatMap[o.CurrentTreadId]
		if ok {
			threadStat.DeleteCount += len(e.Rows)
		}
	}
}

// 统计信息到文件中
func (o *OfflineStat) statToFile() {
	// 表统计
	o.tableStatToFile()

	// thread统计
	o.threadStatToFile()

	// 时间统计
	o.TimestampStatToFile()

	// 事务统计
	o.XidStatToFile()
}

// Write table statistics to file
func (o *OfflineStat) tableStatToFile() {
	stats := make([]*TableBinlogStat, 0, len(o.TotalTableStatMap))
	for _, stat := range o.TotalTableStatMap {
		stats = append(stats, stat)
	}

	sort.Slice(stats, func(i, j int) bool {
		return stats[i].DmlCount() > stats[j].DmlCount()
	})

	filename := o.OfflineStatCfg.TableStatFilePath()
	f, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		seelog.Errorf("Error writing (table) statistics to file. Error opening file. File: %v. %v", filename, err)
		return
	}
	defer f.Close()

	for _, stat := range stats {
		if _, err := f.WriteString(fmt.Sprintf("Table: %v.%v \tdml affected rows: %v, insert: %v, update: %v, delete: %v, table occurrence count: %v\n",
			stat.SchemaName, stat.TableName, stat.DmlCount(), stat.InsertCount, stat.UpdateCount, stat.DeleteCount, stat.AppearCount)); err != nil {
			seelog.Errorf("Error writing (table) statistics. File: %v. Table: %v.%v, %v", filename, stat.SchemaName, stat.TableName, err)
			return
		}
	}
}

func (o *OfflineStat) threadStatToFile() {
	stats := make([]*ThreadBinlogStat, 0, len(o.TotalTableStatMap))
	for _, stat := range o.TotalThreadStatMap {
		stats = append(stats, stat)
	}

	sort.Slice(stats, func(i, j int) bool {
		return stats[i].DmlCount() > stats[j].DmlCount()
	})

	filename := o.OfflineStatCfg.ThreadStatFilePath()
	f, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		seelog.Errorf("Error writing (thread) statistics to file. Error opening file. File: %v. %v", filename, err)
		return
	}
	defer f.Close()

	for _, stat := range stats {
		if _, err := f.WriteString(fmt.Sprintf("threadId: %v\tdml affected rows: %v, insert: %v, update: %v, delete: %v, table occurrence count: %v\n",
			stat.ThreadId, stat.DmlCount(), stat.InsertCount, stat.UpdateCount, stat.DeleteCount, stat.AppearCount)); err != nil {
			seelog.Errorf("Error writing (thread) statistics. File: %v. ThreadId: %v, %v", filename, stat.ThreadId, err)
			return
		}
	}
}

func (o *OfflineStat) TimestampStatToFile() {
	filename := o.OfflineStatCfg.TimestampStatFilePath()
	f, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		seelog.Errorf("Error writing (time) statistics to file. Error opening file. File: %v. %v", filename, err)
		return
	}
	defer f.Close()

	for _, stat := range o.TotalTimestampStats {
		if _, err := f.WriteString(fmt.Sprintf("%v: dml affected rows: %v, insert: %v, update: %v, delete: %v, transaction count: %v, start position: %v\n",
			utils.TS2String(int64(stat.Timestamp), utils.TIME_FORMAT), stat.DmlCount(), stat.InsertCount, stat.UpdateCount, stat.DeleteCount, stat.TxCount, stat.FilePos())); err != nil {
			seelog.Errorf("Error writing (time) statistics. File: %v. %v. Start position: %v. %v", filename, utils.TS2String(int64(stat.Timestamp), utils.TIME_FORMAT), stat.FilePos(), err)
			return
		}
	}
}

func (o *OfflineStat) XidStatToFile() {
	filename := o.OfflineStatCfg.TransactionStatFilePath()
	f, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		seelog.Errorf("Error writing (xid) statistics to file. Error opening file. File: %v. %v", filename, err)
		return
	}
	defer f.Close()

	for _, stat := range o.TotalTransactionStats {
		if _, err := f.WriteString(fmt.Sprintf("Xid: %v \t%v \t dml affected rows: %v, insert: %v, update: %v, delete: %v, start position: %v\n",
			stat.Xid, utils.TS2String(int64(stat.Timestamp), utils.TIME_FORMAT), stat.DmlCount(), stat.InsertCount, stat.UpdateCount, stat.DeleteCount, stat.FilePos())); err != nil {
			seelog.Errorf("Error writing (xid) statistics. File: %v. Xid: %v, %v. Start position: %v. %v", filename, stat.Xid, utils.TS2String(int64(stat.Timestamp), utils.TIME_FORMAT), stat.FilePos(), err)
			return
		}
	}
}
