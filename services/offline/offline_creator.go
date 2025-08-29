package offline

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ChaosHour/mysql-flashback/config"
	"github.com/ChaosHour/mysql-flashback/models"
	"github.com/ChaosHour/mysql-flashback/schema"
	"github.com/ChaosHour/mysql-flashback/utils"
	"github.com/ChaosHour/mysql-flashback/visitor"
	"github.com/cihub/seelog"
	"github.com/go-mysql-org/go-mysql/replication"
)

type OfflineCreator struct {
	OfflineCfg                  *config.OfflineConfig
	CurrentTable                *models.DBTable // 当前的表
	CurrentThreadID             uint32
	CurrentPosition             *models.Position
	CurrentTimestamp            uint32
	CurrentFileIndex            int
	RollBackTableMap            map[string]*schema.Table
	OriRowsEventChan            chan *models.CustomBinlogEvent
	RollbackRowsEventChan       chan *models.CustomBinlogEvent
	Successful                  bool
	OriRowsEventChanClosed      bool
	RollbackRowsEventChanClosed bool
	chanMU                      sync.Mutex
	Qiut                        chan bool
	Quited                      bool
	OriSQLFile                  string
	RollbackSQLFile             string
}

func NewOfflineCreator(offlineConfig *config.OfflineConfig, mTables []*visitor.MatchTable, tableMap map[string]*schema.Table) (*OfflineCreator, error) {
	ct := new(OfflineCreator)
	ct.OfflineCfg = offlineConfig
	ct.OriRowsEventChan = make(chan *models.CustomBinlogEvent, 1000)
	ct.RollbackRowsEventChan = make(chan *models.CustomBinlogEvent, 1000)
	ct.Qiut = make(chan bool)
	ct.CurrentTable = new(models.DBTable)
	ct.RollBackTableMap = make(map[string]*schema.Table)
	ct.CurrentPosition = new(models.Position)

	// 原sql文件
	fileName := ct.getSqlFileName("origin_sql")
	ct.OriSQLFile = fmt.Sprintf("%s/%s", ct.OfflineCfg.GetSaveDir(), fileName)
	seelog.Infof("原sql文件保存路径: %s", ct.OriSQLFile)

	// rollabck sql 文件
	fileName = ct.getSqlFileName("rollback_sql")
	ct.RollbackSQLFile = fmt.Sprintf("%s/%s", ct.OfflineCfg.GetSaveDir(), fileName)
	seelog.Infof("回滚sql文件保存路径: %s", ct.RollbackSQLFile)

	// 获取需要回滚的表
	for _, table := range tableMap {
		key := fmt.Sprintf("%s.%s", table.SchemaName, table.TableName)
		ct.RollBackTableMap[key] = table
	}

	// 设置 需要回滚的 表的字段和条件
	for _, mTable := range mTables {
		rollbackTable, ok := ct.RollBackTableMap[mTable.Table()]
		if !ok {
			seelog.Warnf("match-sql 指定的表没有匹配到. 库:%s, 表:%s", mTable.SchemaName, mTable.TableName)
			continue
		}

		if err := rollbackTable.SetMTableInfo(mTable); err != nil {
			return nil, err
		}
	}

	return ct, nil
}

// 获取保存原sql文件名
func (o *OfflineCreator) getSqlFileName(prefix string) string {
	items := make([]string, 0, 1)

	items = append(items, prefix)

	// 添加时间戳
	items = append(items, strconv.FormatInt(time.Now().UnixNano()/10e6, 10))

	items = append(items, ".sql")

	return strings.Join(items, "_")
}

func (o *OfflineCreator) closeOriChan() {
	o.chanMU.Lock()
	if !o.OriRowsEventChanClosed {
		o.OriRowsEventChanClosed = true
		seelog.Info("生成原sql通道关闭")
		close(o.OriRowsEventChan)
	}
	defer o.chanMU.Unlock()
}

func (o *OfflineCreator) closeRollabckChan() {
	o.chanMU.Lock()
	if !o.RollbackRowsEventChanClosed {
		o.RollbackRowsEventChanClosed = true
		close(o.RollbackRowsEventChan)
		seelog.Info("生成回滚sql通道关闭")
	}
	defer o.chanMU.Unlock()
}

func (o *OfflineCreator) quit() {
	o.chanMU.Lock()
	if !o.Quited {
		o.Quited = true
		close(o.Qiut)
	}
	defer o.chanMU.Unlock()
}

func (o *OfflineCreator) Start() error {
	wg := new(sync.WaitGroup)

	wg.Add(1)
	go o.runProduceEvent(wg)

	wg.Add(1)
	go o.runConsumeEventToOriSQL(wg)

	wg.Add(1)
	go o.runConsumeEventToRollbackSQL(wg)

	wg.Add(1)
	go o.loopPrintProgress(wg)

	wg.Wait()

	return nil
}

func (o *OfflineCreator) runProduceEvent(wg *sync.WaitGroup) {
	defer wg.Done()
	defer func() {
		o.closeOriChan()
		o.closeRollabckChan()
	}()
	seelog.Infof("开始解析binlog文件, 一共有 %v 个文件需要进行解析", len(o.OfflineCfg.BinlogFiles))

	// 创建一个 BinlogParser 对象
	parser := replication.NewBinlogParser()
	for i, binlogFile := range o.OfflineCfg.BinlogFiles {
		o.CurrentFileIndex = i + 1
		seelog.Infof("开始解析binlog文件. 进度: %v/%v, binlog文件: %v", o.CurrentFileIndex, len(o.OfflineCfg.BinlogFiles), binlogFile)
		o.CurrentPosition.File = utils.Filename(binlogFile)

		err := parser.ParseFile(binlogFile, 0, func(event *replication.BinlogEvent) error {
			if o.Quited {
				seelog.Warnf("发现需要退出, 停止产生事件. 文件: %v, pos: %v", binlogFile, event.Header.LogPos)
				return nil
			}
			return o.handleEvent(event)
		})
		if err != nil {
			seelog.Errorf("解析binlog出错. 进度: %v/%v, binlog文件: %v. %v", i+1, len(o.OfflineCfg.BinlogFiles), binlogFile, err)
			o.quit()
			return
		}

		if o.Quited {
			return
		}
	}

	// 正常完成
	seelog.Infof("正常完成解析binlog")
	o.quit()
	o.Successful = true
}

// 处理binlog事件
func (o *OfflineCreator) handleEvent(ev *replication.BinlogEvent) error {
	o.CurrentPosition.Position = uint64(ev.Header.LogPos) // 设置当前位点
	o.CurrentTimestamp = ev.Header.Timestamp

	switch e := ev.Event.(type) {
	case *replication.QueryEvent:
		o.CurrentThreadID = e.SlaveProxyID
	case *replication.TableMapEvent:
		if err := o.handleMapEvent(e); err != nil {
			return err
		}
	case *replication.RowsEvent:
		if err := o.produceRowEvent(ev); err != nil {
			return err
		}
	}

	return nil
}

// 处理 TableMapEvent
func (o *OfflineCreator) handleMapEvent(ev *replication.TableMapEvent) error {
	o.CurrentTable.TableSchema = string(ev.Schema)
	o.CurrentTable.TableName = string(ev.Table)

	return nil
}

// 产生事件
func (o *OfflineCreator) produceRowEvent(ev *replication.BinlogEvent) error {
	// 判断是否是指定的 thread id
	if o.OfflineCfg.ThreadID != 0 && o.OfflineCfg.ThreadID != o.CurrentThreadID {
		//  指定了 thread id, 但是 event thread id 不等于 指定的 thread id
		return nil
	}

	// 判断是否是有过滤相关的event类型
	switch ev.Header.EventType {
	case replication.WRITE_ROWS_EVENTv0, replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2:
		if !o.OfflineCfg.EnableRollbackInsert {
			return nil
		}
	case replication.UPDATE_ROWS_EVENTv0, replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2:
		if !o.OfflineCfg.EnableRollbackUpdate {
			return nil
		}
	case replication.DELETE_ROWS_EVENTv0, replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2:
		if !o.OfflineCfg.EnableRollbackDelete {
			return nil
		}
	}

	// 判断是否指定表要rollback还是所有表要rollback
	if _, ok := o.RollBackTableMap[o.CurrentTable.String()]; !ok {
		return nil
	}

	customEvent := &models.CustomBinlogEvent{
		Event:    ev,
		ThreadId: o.CurrentThreadID,
	}

	o.OriRowsEventChan <- customEvent
	o.RollbackRowsEventChan <- customEvent

	return nil
}

// 消费事件并转化为 执行的 sql
func (o *OfflineCreator) runConsumeEventToOriSQL(wg *sync.WaitGroup) {
	defer wg.Done()

	f, err := os.OpenFile(o.OriSQLFile, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		seelog.Errorf("打开保存原sql文件失败. %s", o.OriSQLFile)
		o.quit()
		return
	}
	defer f.Close()

	for ev := range o.OriRowsEventChan {
		switch e := ev.Event.Event.(type) {
		case *replication.RowsEvent:
			key := fmt.Sprintf("%s.%s", string(e.Table.Schema), string(e.Table.Table))
			t, ok := o.RollBackTableMap[key]
			if !ok {
				seelog.Error("没有获取到表需要回滚的表信息(生成原sql数据的时候) %s.", key)
				continue
			}

			timeStr := utils.TS2String(int64(ev.Event.Header.Timestamp), utils.TIME_FORMAT) // 获取事件时间

			switch ev.Event.Header.EventType {
			case replication.WRITE_ROWS_EVENTv0, replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2:
				if err := o.writeOriInsert(e, f, t, timeStr, ev.ThreadId); err != nil {
					seelog.Error(err.Error())
					o.quit()
					return
				}
			case replication.UPDATE_ROWS_EVENTv0, replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2:
				if err := o.writeOriUpdate(e, f, t, timeStr, ev.ThreadId); err != nil {
					seelog.Error(err.Error())
					o.quit()
					return
				}
			case replication.DELETE_ROWS_EVENTv0, replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2:
				if err := o.writeOriDelete(e, f, t, timeStr, ev.ThreadId); err != nil {
					seelog.Error(err.Error())
					o.quit()
					return
				}
			}
		}
	}
}

// 消费事件并转化为 rollback sql
func (o *OfflineCreator) runConsumeEventToRollbackSQL(wg *sync.WaitGroup) {
	defer wg.Done()

	f, err := os.OpenFile(o.RollbackSQLFile, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		seelog.Errorf("打开保存回滚sql文件失败. %s", o.RollbackSQLFile)
		o.quit()
		return
	}
	defer f.Close()

	for ev := range o.RollbackRowsEventChan {
		switch e := ev.Event.Event.(type) {
		case *replication.RowsEvent:
			key := fmt.Sprintf("%s.%s", string(e.Table.Schema), string(e.Table.Table))
			t, ok := o.RollBackTableMap[key]
			if !ok {
				seelog.Error("没有获取到表需要回滚的表信息(生成回滚sql数据的时候) %s.", key)
				continue
			}

			timeStr := utils.TS2String(int64(ev.Event.Header.Timestamp), utils.TIME_FORMAT) // 获取事件时间

			switch ev.Event.Header.EventType {
			case replication.WRITE_ROWS_EVENTv0, replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2:
				if err := o.writeRollbackDelete(e, f, t, timeStr, ev.ThreadId); err != nil {
					seelog.Error(err.Error())
					o.quit()
					return
				}
			case replication.UPDATE_ROWS_EVENTv0, replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2:
				if err := o.writeRollbackUpdate(e, f, t, timeStr, ev.ThreadId); err != nil {
					seelog.Error(err.Error())
					o.quit()
					return
				}
			case replication.DELETE_ROWS_EVENTv0, replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2:
				if err := o.writeRollbackInsert(e, f, t, timeStr, ev.ThreadId); err != nil {
					seelog.Error(err.Error())
					o.quit()
					return
				}
			}
		}
	}
}

// 生成insert的原生sql并切入文件
func (o *OfflineCreator) writeOriInsert(
	ev *replication.RowsEvent,
	f *os.File,
	tbl *schema.Table,
	timeStr string,
	threadId uint32,
) error {
	for _, row := range ev.Rows {
		// 过滤该行数据是否匹配 指定的where条件
		if ok := tbl.FilterRow(row); !ok {
			continue
		}

		// 获取主键值的 crc32 值
		crc32 := tbl.GetPKCrc32(row)

		// 获取最终的placeholder的sql语句   %s %s -> %#v %s
		insertTemplate := utils.ReplaceSqlPlaceHolder(tbl.InsertTemplate, row, crc32, timeStr, threadId)
		// 获取PK数据  "aaa", nil -> "aaa", "NULL"
		data, err := utils.ConverSQLType(row)
		if err != nil {
			return fmt.Errorf("[writeOriInsert] 将一行所有字段数据转化成sql字符串出错. %v", err.Error())
		}
		// 将模板和数据组合称最终的SQL
		sqlStr := fmt.Sprintf(insertTemplate, data...)
		if _, err := f.WriteString(sqlStr); err != nil {
			return fmt.Errorf("[writeOriInsert] 将sql写入文件出错. %v", err.Error())
		}
	}
	return nil
}

// 生成update的原生sql并切入文件
func (o *OfflineCreator) writeOriUpdate(
	ev *replication.RowsEvent,
	f *os.File,
	tbl *schema.Table,
	timeStr string,
	threadId uint32,
) error {
	recordCount := len(ev.Rows) / 2 // 有多少记录被update
	for i := 0; i < recordCount; i++ {
		whereIndex := i * 2        // where条件下角标(old记录值)
		setIndex := whereIndex + 1 // set条件下角标(new记录值)

		// 过滤该行数据是否匹配 指定的where条件
		if ok := tbl.FilterRow(ev.Rows[whereIndex]); !ok {
			continue
		}

		// 设置获取set子句的值
		setUseRow, err := tbl.GetUseRow(ev.Rows[setIndex])
		if err != nil {
			seelog.Errorf("%v. Update Ori 表: %v. 字段: %v. binlog数据: %v", err, tbl.TableName, tbl.UseColumnNames, ev.Rows[setIndex])
			continue
		}

		placeholderValues := make([]interface{}, len(setUseRow)+len(tbl.PKColumnNames))
		copy(placeholderValues, setUseRow)

		// 设置获取where子句的值
		tbl.SetPKValues(ev.Rows[whereIndex], placeholderValues[len(setUseRow):])

		// 获取主键值的 crc32 值
		crc32 := tbl.GetPKCrc32(ev.Rows[whereIndex])

		// 获取最终的　update 模板
		updateTemplate := utils.ReplaceSqlPlaceHolder(tbl.UpdateTemplate, placeholderValues, crc32, timeStr, threadId)
		data, err := utils.ConverSQLType(placeholderValues)
		if err != nil {
			return fmt.Errorf("[writeOriUpdate] 将一行所有字段数据转化成sql字符串出错. %v", err.Error())
		}

		sql := fmt.Sprintf(updateTemplate, data...)
		if _, err := f.WriteString(sql); err != nil {
			return fmt.Errorf("[writeOriUpdate] 将sql写入文件出错. %v", err.Error())
		}
	}
	return nil
}

// 生成update的原生sql并切入文件
func (o *OfflineCreator) writeOriDelete(
	ev *replication.RowsEvent,
	f *os.File,
	tbl *schema.Table,
	timeStr string,
	threadId uint32,
) error {
	for _, row := range ev.Rows {
		// 过滤该行数据是否匹配 指定的where条件
		if ok := tbl.FilterRow(row); !ok {
			continue
		}

		placeholderValues := make([]interface{}, len(tbl.PKColumnNames))
		// 设置获取where子句的值
		tbl.SetPKValues(row, placeholderValues)

		// 获取主键值的 crc32 值
		crc32 := tbl.GetPKCrc32(row)

		// 获取最终的placeholder的sql语句   %s %s -> %#v %s
		deleteTemplate := utils.ReplaceSqlPlaceHolder(tbl.DeleteTemplate, placeholderValues, crc32, timeStr, threadId)
		// 获取PK数据  "aaa", nil -> "aaa", "NULL"
		pkData, err := utils.ConverSQLType(placeholderValues)
		if err != nil {
			return fmt.Errorf("[writeOriDelete] 将主键字段数据转化成sql字符串出错. %v", err.Error())
		}
		// 将模板和数据组合称最终的SQL
		sqlStr := fmt.Sprintf(deleteTemplate, pkData...)

		if _, err := f.WriteString(sqlStr); err != nil {
			return fmt.Errorf("[writeOriDelete] 将sql写入文件出错. %v", err.Error())
		}
	}
	return nil
}

// 生成insert的回滚sql并切入文件
func (o *OfflineCreator) writeRollbackInsert(
	ev *replication.RowsEvent,
	f *os.File,
	tbl *schema.Table,
	timeStr string,
	threadId uint32,
) error {
	for _, row := range ev.Rows {
		// 过滤该行数据是否匹配 指定的where条件
		if ok := tbl.FilterRow(row); !ok {
			continue
		}

		// 获取主键值的 crc32 值
		crc32 := tbl.GetPKCrc32(row)

		// 获取最终的placeholder的sql语句   %s %s -> %#v %s
		insertTemplate := utils.ReplaceSqlPlaceHolder(tbl.InsertTemplate, row, crc32, timeStr, threadId)
		// 获取PK数据  "aaa", nil -> "aaa", "NULL"
		data, err := utils.ConverSQLType(row)
		if err != nil {
			return fmt.Errorf("[writeRollbackInsert] 将一行所有字段数据转化成sql字符串出错. %v", err.Error())
		}
		// 将模板和数据组合称最终的SQL
		sqlStr := fmt.Sprintf(insertTemplate, data...)
		if _, err := f.WriteString(sqlStr); err != nil {
			return fmt.Errorf("[writeRollbackInsert] 将sql写入文件出错. %v", err.Error())
		}
	}
	return nil
}

// 生成update的回滚sql并切入文件
func (o *OfflineCreator) writeRollbackUpdate(
	ev *replication.RowsEvent,
	f *os.File,
	tbl *schema.Table,
	timeStr string,
	threadId uint32,
) error {
	recordCount := len(ev.Rows) / 2 // 有多少记录被update
	for i := 0; i < recordCount; i++ {
		setIndex := i * 2          // set条件下角标(old记录值)
		whereIndex := setIndex + 1 // where条件下角标(new记录值)

		// 过滤该行数据是否匹配 指定的where条件
		if ok := tbl.FilterRow(ev.Rows[setIndex]); !ok {
			continue
		}

		// 设置获取set子句的值
		setUseRow, err := tbl.GetUseRow(ev.Rows[setIndex])
		if err != nil {
			seelog.Errorf("%v. Update Rollback 表: %v. 字段: %v. binlog数据: %v", err, tbl.TableName, tbl.UseColumnNames, ev.Rows[setIndex])
			continue
		}

		placeholderValues := make([]interface{}, len(setUseRow)+len(tbl.PKColumnNames))
		copy(placeholderValues, setUseRow)

		// 设置获取where子句的值
		tbl.SetPKValues(ev.Rows[whereIndex], placeholderValues[len(setUseRow):])

		// 获取主键值的 crc32 值
		crc32 := tbl.GetPKCrc32(ev.Rows[whereIndex])

		updateTemplate := utils.ReplaceSqlPlaceHolder(tbl.UpdateTemplate, placeholderValues, crc32, timeStr, threadId)
		data, err := utils.ConverSQLType(placeholderValues)
		if err != nil {
			return fmt.Errorf("[writeRollbackUpdate] 将一行所有字段数据转化成sql字符串出错. %v", err.Error())
		}
		sqlStr := fmt.Sprintf(updateTemplate, data...)
		if _, err := f.WriteString(sqlStr); err != nil {
			return fmt.Errorf("[writeRollbackUpdate] 将sql写入文件出错. %v", err.Error())
		}
	}
	return nil
}

// 生成update的回滚sql并切入文件
func (o *OfflineCreator) writeRollbackDelete(
	ev *replication.RowsEvent,
	f *os.File,
	tbl *schema.Table,
	timeStr string,
	threadId uint32,
) error {
	for _, row := range ev.Rows {
		// 过滤该行数据是否匹配 指定的where条件
		if ok := tbl.FilterRow(row); !ok {
			continue
		}

		placeholderValues := make([]interface{}, len(tbl.PKColumnNames))
		// 设置获取where子句的值
		tbl.SetPKValues(row, placeholderValues)

		// 获取主键值的 crc32 值
		crc32 := tbl.GetPKCrc32(row)

		// 获取最终的placeholder的sql语句   %s %s -> %#v %s
		deleteTemplate := utils.ReplaceSqlPlaceHolder(tbl.DeleteTemplate, placeholderValues, crc32, timeStr, threadId)
		// 获取PK数据  "aaa", nil -> "aaa", "NULL"
		pkData, err := utils.ConverSQLType(placeholderValues)
		if err != nil {
			return fmt.Errorf("[writeRollbackDelete] 将主键字段数据转化成sql字符串出错. %v", err.Error())
		}
		// 将模板和数据组合称最终的SQL
		sqlStr := fmt.Sprintf(deleteTemplate, pkData...)

		if _, err := f.WriteString(sqlStr); err != nil {
			return fmt.Errorf("[writeRollbackDelete] 将sql写入文件出错. %v", err.Error())
		}
	}
	return nil
}

// 获取进度信息
func (o *OfflineCreator) getProgress() string {
	return fmt.Sprintf("进度: %v/%v. 当前位点: %v", o.CurrentFileIndex, len(o.OfflineCfg.BinlogFiles), o.CurrentPosition.String())
}

func (o *OfflineCreator) loopPrintProgress(wg *sync.WaitGroup) {
	defer wg.Done()

	ticker := time.NewTicker(10 * time.Second)
	for {
		select {
		case _, ok := <-o.Qiut:
			if !ok {
				seelog.Info("停止打印进度信息")
			}
			ticker.Stop()
			return
		case <-ticker.C:
			seelog.Infof(o.getProgress())
		}
	}
}
