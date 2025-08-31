package create

import (
	"context"
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
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
)

const (
	START_POS_BY_NONE uint8 = iota
	START_POS_BY_POS
	START_POS_BY_TIME
)

type Creator struct {
	CC               *config.CreateConfig
	DBC              *config.DBConfig
	Syncer           *replication.BinlogSyncer
	CurrentTable     *models.DBTable // Current table
	StartPosition    *models.Position
	EndPosition      *models.Position
	CurrentPosition  *models.Position
	GetStartPosType  uint8
	CurrentTimestamp uint32
	CurrentThreadID  uint32
	StartTime        time.Time
	StartTimestamp   uint32
	HaveEndPosition  bool
	EndTime          time.Time
	HaveEndTime      bool
	EndTimestamp     uint32
	RollBackTableMap map[string]*schema.Table
	RollbackType
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

func NewFlashback(sc *config.CreateConfig, dbc *config.DBConfig, mTables []*visitor.MatchTable) (*Creator, error) {
	var err error

	ct := new(Creator)
	ct.CC = sc
	ct.DBC = dbc
	ct.OriRowsEventChan = make(chan *models.CustomBinlogEvent, 1000)
	ct.RollbackRowsEventChan = make(chan *models.CustomBinlogEvent, 1000)
	ct.Qiut = make(chan bool)
	ct.CurrentTable = new(models.DBTable)
	ct.CurrentPosition = new(models.Position)
	ct.RollBackTableMap = make(map[string]*schema.Table)
	ct.StartPosition, err = GetStartPosition(ct.CC, ct.DBC)
	if err != nil {
		return nil, err
	}

	// Get the type of position acquisition, whether by position or by time
	if ct.CC.HaveStartPosInfo() { // Get start position through specified position
		ct.GetStartPosType = START_POS_BY_POS
		seelog.Infof("Parse binlog start position obtained through specified start binlog position. Start position: %s", ct.StartPosition.String())
	} else if ct.CC.HaveStartTime() { // Get start position through time
		ct.GetStartPosType = START_POS_BY_TIME
		seelog.Infof("Parse binlog start position obtained through specified start time. Start position: %s", ct.StartPosition.String())
		ct.StartTime, err = utils.NewTime(ct.CC.StartTime)
		if err != nil {
			return nil, fmt.Errorf("the input start time has a problem. %v", err)
		}
		ct.StartTimestamp = uint32(ct.StartTime.Unix())
	} else {
		return nil, fmt.Errorf("unable to obtain")
	}

	// Original SQL file
	fileName := ct.getSqlFileName("origin_sql")
	ct.OriSQLFile = fmt.Sprintf("%s/%s", ct.CC.GetSaveDir(), fileName)
	seelog.Infof("Original SQL file save path: %s", ct.OriSQLFile)

	// Rollback SQL file
	fileName = ct.getSqlFileName("rollback_sql")
	ct.RollbackSQLFile = fmt.Sprintf("%s/%s", ct.CC.GetSaveDir(), fileName)
	seelog.Infof("Rollback SQL file save path: %s", ct.RollbackSQLFile)

	if ct.CC.HaveEndPosInfo() { // Determine and assign end position
		ct.HaveEndPosition = true
		ct.EndPosition = &models.Position{
			File:     ct.CC.EndLogFile,
			Position: ct.CC.EndLogPos,
		}
		lastPos, err := GetAndGeneraLastEvent()
		if err != nil {
			return nil, err
		}
		if lastPos.LessThan(ct.EndPosition) {
			return nil, fmt.Errorf("the specified end position [%s] has not arrived yet", ct.EndPosition.String())
		}
	} else if ct.CC.HaveEndTime() { // 判断赋值结束时间
		ct.HaveEndTime = true
		ct.EndTime, err = utils.NewTime(ct.CC.EndTime)
		if err != nil {
			return nil, fmt.Errorf("输入的结束时间有问题. %v", err)
		}
		ct.EndTimestamp = uint32(ct.EndTime.Unix())
		_, err := GetAndGeneraLastEvent()
		if err != nil {
			return nil, err
		}
	}

	// Get tables that need rollback
	rollbackTables, rollbackType, err := FindRollbackTables(ct.CC, mTables)
	if err != nil {
		return nil, err
	}
	ct.RollbackType = rollbackType
	if ct.RollbackType == RollbackPartialTable { // Need to rollback partial tables
		for _, table := range rollbackTables {
			if err = ct.cacheRollbackTable(table.TableSchema, table.TableName); err != nil {
				return nil, err
			}
		}

		// Set fields and conditions for tables that need rollback
		for _, mTable := range mTables {
			rollbackTable, ok := ct.RollBackTableMap[mTable.Table()]
			if !ok {
				seelog.Warnf("Table specified by match-sql not matched. Schema:%s, Table:%s", mTable.SchemaName, mTable.TableName)
				continue
			}

			if err = rollbackTable.SetMTableInfo(mTable); err != nil {
				return nil, err
			}
		}
	}

	// 设置获取 sync
	cfg := dbc.GetSyncerConfig()
	ct.Syncer = replication.NewBinlogSyncer(cfg)

	return ct, nil
}

// 保存需要进行rollback的表
func (c *Creator) cacheRollbackTable(sName string, tName string) error {
	key := fmt.Sprintf("%s.%s", sName, tName)
	t, err := schema.NewTable(sName, tName)
	if err != nil {
		return err
	}

	c.RollBackTableMap[key] = t

	return nil
}

func (c *Creator) closeOriChan() {
	c.chanMU.Lock()
	if !c.OriRowsEventChanClosed {
		c.OriRowsEventChanClosed = true
		seelog.Info("生成原sql通道关闭")
		close(c.OriRowsEventChan)
	}
	defer c.chanMU.Unlock()
}

func (c *Creator) closeRollabckChan() {
	c.chanMU.Lock()
	if !c.RollbackRowsEventChanClosed {
		c.RollbackRowsEventChanClosed = true
		close(c.RollbackRowsEventChan)
		seelog.Info("Rollback SQL generation channel closed")
	}
	defer c.chanMU.Unlock()
}

func (c *Creator) quit() {
	c.chanMU.Lock()
	if !c.Quited {
		c.Quited = true
		close(c.Qiut)
	}
	defer c.chanMU.Unlock()
}

func (c *Creator) Start() error {
	wg := new(sync.WaitGroup)

	c.saveInfo(false)

	wg.Add(1)
	go c.runProduceEvent(wg)

	wg.Add(1)
	go c.runConsumeEventToOriSQL(wg)

	wg.Add(1)
	go c.runConsumeEventToRollbackSQL(wg)

	wg.Add(1)
	go c.loopSaveInfo(wg)

	wg.Wait()

	c.saveInfo(true)

	return nil
}

func (c *Creator) runProduceEvent(wg *sync.WaitGroup) {
	defer wg.Done()
	defer c.Syncer.Close()

	// Determine if skipping is needed, position
	var isSkip bool
	if c.GetStartPosType == START_POS_BY_TIME { // If start position is obtained through time, skipping needs to be executed
		isSkip = true
	}
	seelog.Debugf("Whether events need to be skipped: %v", isSkip)

	pos := mysql.Position{Name: c.StartPosition.File, Pos: uint32(c.StartPosition.Position)}
	streamer, err := c.Syncer.StartSync(pos)
	if err != nil {
		seelog.Error(err.Error())
		return
	}
produceLoop:
	for { // Iterate through events to get the second available timestamp
		select {
		case _, ok := <-c.Qiut:
			if !ok {
				seelog.Errorf("Stop generating events")
				break produceLoop
			}
		default:
			ev, err := streamer.GetEvent(context.Background())
			if err != nil {
				seelog.Error(err.Error())
				c.quit()
			}

			// Skip events that haven't reached the start time yet
			if isSkip {
				// Determine if the start time has been reached
				if ev.Header.Timestamp < c.StartTimestamp {
					continue
				} else {
					isSkip = false
					seelog.Infof("Stop skipping, start generating rollback SQL. Timestamp: %d, Time: %s, Position: %s:%d", ev.Header.Timestamp,
						utils.TS2String(int64(ev.Header.Timestamp), utils.TIME_FORMAT), c.StartPosition.File, ev.Header.LogPos)
				}
			}

			if err = c.handleEvent(ev); err != nil {
				seelog.Error(err.Error())
				c.quit()
			}
		}
	}

	c.closeOriChan()
	c.closeRollabckChan()
}

// 处理binlog事件
func (c *Creator) handleEvent(ev *replication.BinlogEvent) error {
	c.CurrentPosition.Position = uint64(ev.Header.LogPos) // 设置当前位点
	c.CurrentTimestamp = ev.Header.Timestamp

	// 判断是否到达了结束位点
	if err := c.rlEndPos(); err != nil {
		return err
	}

	switch e := ev.Event.(type) {
	case *replication.RotateEvent:
		c.CurrentPosition.File = string(e.NextLogName)
		// 判断是否到达了结束位点
		if err := c.rlEndPos(); err != nil {
			return err
		}
	case *replication.QueryEvent:
		c.CurrentThreadID = e.SlaveProxyID
	case *replication.TableMapEvent:
		if err := c.handleMapEvent(e); err != nil {
			return err
		}
	case *replication.RowsEvent:
		if err := c.produceRowEvent(ev); err != nil {
			return err
		}
	}

	return nil
}

// 大于结束位点
func (c *Creator) rlEndPos() error {
	// 判断是否超过了指定位点
	if c.HaveEndPosition {
		if c.EndPosition.LessThan(c.CurrentPosition) {
			c.Successful = true // 代表任务完成
			return fmt.Errorf("当前使用位点 %s 已经超过指定的停止位点 %s. 任务停止",
				c.CurrentPosition.String(), c.EndPosition.String())
		}
	} else if c.HaveEndTime { // 使用事件是否超过了结束时间
		if c.EndTimestamp < c.CurrentTimestamp {
			c.Successful = true // 代表任务完成
			return fmt.Errorf("当前使用时间 %s 已经超过指定的停止时间 %s. 任务停止",
				utils.TS2String(int64(c.CurrentTimestamp), utils.TIME_FORMAT),
				utils.TS2String(int64(c.EndTimestamp), utils.TIME_FORMAT))
		}
	} else {
		return fmt.Errorf("没有指定结束时间和结束位点")
	}

	return nil
}

// 处理 TableMapEvent
func (c *Creator) handleMapEvent(ev *replication.TableMapEvent) error {
	c.CurrentTable.TableSchema = string(ev.Schema)
	c.CurrentTable.TableName = string(ev.Table)

	// 判断是否所有的表都要进行rollback 并且缓存没有缓存的表
	if c.RollbackType == RollbackAllTable {
		if _, ok := c.RollBackTableMap[c.CurrentTable.String()]; !ok {
			if err := c.cacheRollbackTable(c.CurrentTable.TableSchema, c.CurrentTable.TableName); err != nil {
				return err
			}
		}
	}
	return nil
}

// 产生事件
func (c *Creator) produceRowEvent(ev *replication.BinlogEvent) error {
	// 判断是否是指定的 thread id
	if c.CC.ThreadID != 0 && c.CC.ThreadID != c.CurrentThreadID {
		//  指定了 thread id, 但是 event thread id 不等于 指定的 thread id
		return nil
	}

	// 判断是否是有过滤相关的event类型
	switch ev.Header.EventType {
	case replication.WRITE_ROWS_EVENTv0, replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2:
		if !c.CC.EnableRollbackInsert {
			return nil
		}
	case replication.UPDATE_ROWS_EVENTv0, replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2:
		if !c.CC.EnableRollbackUpdate {
			return nil
		}
	case replication.DELETE_ROWS_EVENTv0, replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2:
		if !c.CC.EnableRollbackDelete {
			return nil
		}
	}

	// 判断是否指定表要rollback还是所有表要rollback
	if c.RollbackType == RollbackPartialTable {
		if _, ok := c.RollBackTableMap[c.CurrentTable.String()]; !ok {
			return nil
		}
	}

	customEvent := &models.CustomBinlogEvent{
		Event:    ev,
		ThreadId: c.CurrentThreadID,
	}

	c.OriRowsEventChan <- customEvent
	c.RollbackRowsEventChan <- customEvent

	return nil
}

// 消费事件并转化为 执行的 sql
func (c *Creator) runConsumeEventToOriSQL(wg *sync.WaitGroup) {
	defer wg.Done()

	f, err := os.OpenFile(c.OriSQLFile, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		seelog.Errorf("打开保存原sql文件失败. %s", c.OriSQLFile)
		c.quit()
		return
	}
	defer f.Close()

	for ev := range c.OriRowsEventChan {
		switch e := ev.Event.Event.(type) {
		case *replication.RowsEvent:
			key := fmt.Sprintf("%s.%s", string(e.Table.Schema), string(e.Table.Table))
			t, ok := c.RollBackTableMap[key]
			if !ok {
				seelog.Error("Table rollback information not obtained (when generating original SQL data) %s.", key)
				continue
			}

			timeStr := utils.TS2String(int64(ev.Event.Header.Timestamp), utils.TIME_FORMAT) // 获取事件时间

			switch ev.Event.Header.EventType {
			case replication.WRITE_ROWS_EVENTv0, replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2:
				if err := c.writeOriInsert(e, f, t, timeStr, ev.ThreadId); err != nil {
					seelog.Error(err.Error())
					c.quit()
					return
				}
			case replication.UPDATE_ROWS_EVENTv0, replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2:
				if err := c.writeOriUpdate(e, f, t, timeStr, ev.ThreadId); err != nil {
					seelog.Error(err.Error())
					c.quit()
					return
				}
			case replication.DELETE_ROWS_EVENTv0, replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2:
				if err := c.writeOriDelete(e, f, t, timeStr, ev.ThreadId); err != nil {
					seelog.Error(err.Error())
					c.quit()
					return
				}
			}
		}
	}
}

// 消费事件并转化为 rollback sql
func (c *Creator) runConsumeEventToRollbackSQL(wg *sync.WaitGroup) {
	defer wg.Done()

	f, err := os.OpenFile(c.RollbackSQLFile, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		seelog.Errorf("Failed to open rollback SQL save file. %s", c.RollbackSQLFile)
		c.quit()
		return
	}
	defer f.Close()

	for ev := range c.RollbackRowsEventChan {
		switch e := ev.Event.Event.(type) {
		case *replication.RowsEvent:
			key := fmt.Sprintf("%s.%s", string(e.Table.Schema), string(e.Table.Table))
			t, ok := c.RollBackTableMap[key]
			if !ok {
				seelog.Error("Table rollback information not obtained (when generating rollback SQL data) %s.", key)
				continue
			}

			timeStr := utils.TS2String(int64(ev.Event.Header.Timestamp), utils.TIME_FORMAT) // 获取事件时间

			switch ev.Event.Header.EventType {
			case replication.WRITE_ROWS_EVENTv0, replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2:
				if err := c.writeRollbackDelete(e, f, t, timeStr, ev.ThreadId); err != nil {
					seelog.Error(err.Error())
					c.quit()
					return
				}
			case replication.UPDATE_ROWS_EVENTv0, replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2:
				if err := c.writeRollbackUpdate(e, f, t, timeStr, ev.ThreadId); err != nil {
					seelog.Error(err.Error())
					c.quit()
					return
				}
			case replication.DELETE_ROWS_EVENTv0, replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2:
				if err := c.writeRollbackInsert(e, f, t, timeStr, ev.ThreadId); err != nil {
					seelog.Error(err.Error())
					c.quit()
					return
				}
			}
		}
	}
}

// 生成insert的原生sql并切入文件
func (c *Creator) writeOriInsert(
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
func (c *Creator) writeOriUpdate(
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
func (c *Creator) writeOriDelete(
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

// Generate rollback SQL for insert and write to file
func (c *Creator) writeRollbackInsert(
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

// Generate rollback SQL for update and write to file
func (c *Creator) writeRollbackUpdate(
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

// Generate rollback SQL for delete and write to file
func (c *Creator) writeRollbackDelete(
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

// 获取保存原sql文件名
func (c *Creator) getSqlFileName(prefix string) string {
	items := make([]string, 0, 1)

	items = append(items, c.DBC.Host)
	items = append(items, strconv.FormatInt(int64(c.DBC.Port), 10))
	items = append(items, prefix)
	// Start position
	items = append(items, c.StartPosition.File)
	items = append(items, strconv.FormatInt(int64(c.StartPosition.Position), 10))

	// End position or event
	if c.HaveEndPosition {
		items = append(items, c.EndPosition.File)
		items = append(items, strconv.FormatInt(int64(c.EndPosition.Position), 10))
	} else if c.HaveEndTime {
		items = append(items, utils.TS2String(int64(c.EndTimestamp), utils.TIME_FORMAT_FILE_NAME))
	}

	// Add timestamp
	items = append(items, strconv.FormatInt(time.Now().UnixNano()/10e6, 10))

	items = append(items, ".sql")

	return strings.Join(items, "_")
}

// Save related data
func (c *Creator) saveInfo(complete bool) {
	progress, progressInfo := c.getProgress(complete)

	seelog.Warnf("Progress: %f, Progress info: %s", progress, progressInfo)
}

// Get progress information
func (c *Creator) getProgress(complete bool) (float64, string) {
	var progress float64
	var progressInfo string
	var total int64
	var current int64
	if c.HaveEndPosition {
		total = c.EndPosition.GetTotalNum() - c.StartPosition.GetTotalNum()
		if c.CurrentPosition.GetTotalNum() == 0 {
			current = 0
		} else {
			current = c.CurrentPosition.GetTotalNum() - c.StartPosition.GetTotalNum()
		}
		progressInfo = fmt.Sprintf("Start position:%s, Current position:%s, End position:%s", c.StartPosition.String(), c.CurrentPosition.String(), c.EndPosition.String())
	} else if c.HaveEndTime {
		if c.CC.HaveStartTime() { // Have start time
			startTimestamp, err := utils.StrTime2Int(c.CC.StartTime)
			if err != nil {
				startTimestamp = 0
			}
			total = int64(c.EndTimestamp) - startTimestamp
			current = int64(c.CurrentTimestamp) - startTimestamp
			progressInfo = fmt.Sprintf("Start time:%s, Current time:%s, End time:%s", c.CC.StartTime, utils.TS2String(int64(c.CurrentTimestamp), utils.TIME_FORMAT), c.CC.EndTime)
		} else { // No start time
			total = int64(c.EndTimestamp)
			current = int64(c.CurrentTimestamp)
			progressInfo = fmt.Sprintf("Start position:%s, Current time:%s, End time:%s", c.StartPosition.String(), utils.TS2String(int64(c.CurrentTimestamp), utils.TIME_FORMAT), c.CC.EndTime)
		}
	} else {
		seelog.Errorf("Unable to get task progress, no end time and end position specified")
	}

	if total != 0 {
		progress = float64(current) / float64(total) * 100
	}

	if complete {
		progress = 100
	} else {
		if progress >= 100 {
			progress = 99.99
		}
	}

	return progress, progressInfo
}

func (c *Creator) loopSaveInfo(wg *sync.WaitGroup) {
	defer wg.Done()

	ticker := time.NewTicker(10 * time.Second)
	for {
		select {
		case _, ok := <-c.Qiut:
			if !ok {
				seelog.Info("停止保存进度信息")
			}
			ticker.Stop()
			return
		case <-ticker.C:
			c.saveInfo(false)
		}
	}
}
