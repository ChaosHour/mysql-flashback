package execute

import (
	"context"
	"fmt"
	"github.com/cihub/seelog"
	"github.com/ChaosHour/mysql-flashback/config"
	"github.com/ChaosHour/mysql-flashback/dao"
	"github.com/ChaosHour/mysql-flashback/utils"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	DEFAULT_READ_SIZE = 8192
)

type SqlContext struct {
	Sql string
	Tag int64
}

func NewSqlContext(sql string, tag int64) *SqlContext {
	return &SqlContext{
		Sql: sql,
		Tag: tag,
	}
}

type Executor struct {
	ec           *config.ExecuteConfig
	dbc          *config.DBConfig
	sqlChans     []chan *SqlContext
	chanExecNums []int64
	ctx          context.Context
	cancal       context.CancelFunc
	EmitSuccess  bool
	ExecSuccess  bool
	ParseCount   int64
	ExecCount    int64
}

func NewExecutor(ec *config.ExecuteConfig, dbc *config.DBConfig) *Executor {
	executor := new(Executor)
	executor.ec = ec
	executor.dbc = dbc
	executor.ctx, executor.cancal = context.WithCancel(context.Background())
	executor.sqlChans = make([]chan *SqlContext, 0, ec.Paraller)
	executor.chanExecNums = make([]int64, 0, ec.Paraller)

	for i := int64(0); i < ec.Paraller; i++ {
		executor.sqlChans = append(executor.sqlChans, make(chan *SqlContext, 1000))
		executor.chanExecNums = append(executor.chanExecNums, 0)
	}

	return executor
}

func (e *Executor) closeSQLChans() {
	for _, sqlChan := range e.sqlChans {
		close(sqlChan)
	}
}

func (e *Executor) Start() error {
	e.saveInfo(false)

	go e.loopSaveInfo()
	wg := new(sync.WaitGroup)

	wg.Add(1)
	go e.readFile(wg)

	for i := int64(0); i < e.ec.Paraller; i++ {
		wg.Add(1)
		go e.execSQL(wg, e.sqlChans[i], &(e.chanExecNums[i]), i)
	}

	wg.Wait()

	if e.EmitSuccess && e.ExecSuccess { // 成功执行
		e.saveInfo(true)
	}

	for i, num := range e.chanExecNums {
		seelog.Infof("协程 %d, 最后执行的Sql号为: %d", i, num)
	}

	return nil
}

// 倒序读取文件
func (e *Executor) readFile(wg *sync.WaitGroup) {
	defer wg.Done()

	fileInfo, err := os.Stat(e.ec.FilePath)
	if err != nil {
		seelog.Errorf("获取文件信息失败: %s. %v", e.ec.FilePath, err.Error())
		e.closeSQLChans()
		return
	}
	f, err := os.Open(e.ec.FilePath)
	if err != nil {
		seelog.Errorf("打开回滚sql文件失败: %s. %v", e.ec.FilePath, err.Error())
		e.closeSQLChans()
		return
	}
	defer f.Close()

	defautBufSize := int64(DEFAULT_READ_SIZE)
	unReadSize := fileInfo.Size()
	part := make([]byte, defautBufSize)
	lastRecords := make([]string, 0, 1)

	for ; unReadSize >= 0; unReadSize -= defautBufSize {
		select {
		case <-e.ctx.Done():
			e.closeSQLChans()
			return
		default:
		}
		if err = e.generalSQL(f, unReadSize, defautBufSize, &part, &lastRecords); err != nil {
			seelog.Errorf("打开回滚sql文件失败: %s. %v", e.ec.FilePath, err.Error())
			e.closeSQLChans()
			return
		}
	}
	e.emitSQL(lastRecords)
	e.EmitSuccess = true
	e.closeSQLChans()
}

// 倒序读取每个sql, 算法比较复杂, 要是出错, 我也看不懂了
func (e *Executor) generalSQL(
	f *os.File,
	unReadSize int64,
	defautBufSize int64,
	part *([]byte),
	lastRecords *([]string),
) error {
	// 获取每一个bolck的byte
	offset := unReadSize - defautBufSize
	if offset <= 0 {
		*part = make([]byte, defautBufSize+offset)
		offset = 0
	}
	_, err := f.ReadAt(*part, offset)
	if err != nil {
		return err
	}

	sepCount := 0
	afterIndex := int64(len(*part))
	for i := int64(len(*part)) - 1; i >= 0; i-- {
		if (*part)[i] == 10 { // 遇到了分割符
			sepCount++
			if sepCount == 1 { // 第一次碰到分隔符
				if i != int64(len(*part))-1 { // block的最后一个字符不是换行,
					*lastRecords = append(*lastRecords, string((*part)[i+1:afterIndex]))
				}
				e.emitSQL(*lastRecords)
				*lastRecords = make([]string, 0, 1)
				afterIndex = i
				continue
			}

			// 本次block不是第1次碰到分隔符, 说明block中间有完整的sql
			if i == int64(len(*part))-1 { // block的最后一个字符是换行,
				e.emitSQL([]string{string((*part)[i:afterIndex])})
			} else {
				e.emitSQL([]string{string((*part)[i+1 : afterIndex])})
			}
			afterIndex = i
			continue
		}

		// 没有碰到分隔符
	}

	// 分割符没有在block第一个字符中
	if afterIndex != 0 { // 该block有部分剩余数据
		*lastRecords = append(*lastRecords, string((*part)[0:afterIndex]))
		return nil
	}

	if sepCount == 0 { // 整个block都没有陪到分割符的情况
		*lastRecords = append(*lastRecords, string(*part))
		return nil
	}

	return nil
}

func (e *Executor) emitSQL(sqlItems []string) error {
	if len(sqlItems) == 0 {
		return nil
	}

	sql := ""
	for i := len(sqlItems) - 1; i >= 0; i-- {
		sql += string(sqlItems[i])
	}

	// 获取需要向哪个 chain 进行发送sql
	slot := e.getSlot(sql, e.ec.Paraller)

	e.ParseCount++

	sqlContext := NewSqlContext(sql, e.ParseCount)

	e.sqlChans[slot] <- sqlContext
	return nil
}

func (e *Executor) execSQL(wg *sync.WaitGroup, sqlChan chan *SqlContext, sqlExecNum *int64, tag int64) {
	defer wg.Done()

	seelog.Infof("启动第 %d 个指定回滚sql协程", tag)
	d := dao.NewDefaultDao()
	for sqlCtx := range sqlChan {
		if err := d.ExecDML(sqlCtx.Sql); err != nil {
			seelog.Errorf("第%d条sql执行回滚失败. %s. %s", sqlCtx.Tag, sqlCtx.Sql, err.Error())
		}

		e.IncrCount()
		*sqlExecNum = sqlCtx.Tag
	}

	e.ExecSuccess = true
}

func (e *Executor) IncrCount() {
	atomic.AddInt64(&e.ExecCount, 1)
}

func (e *Executor) getSlot(sql string, mod int64) int64 {
	comment := utils.GetSQLStmtHearderComment(&sql)
	if strings.TrimSpace(comment) == "" {
		return 0
	}

	// crc32:234234, 2020-10-10 00:00:00, threadId:222
	items := strings.Split(comment, ",")
	if len(items) < 1 {
		return 0
	}

	// crc32:234234
	crc32Items := strings.Split(strings.TrimSpace(items[0]), ":")
	if len(crc32Items) != 2 {
		return 0
	}

	crc32, err := strconv.ParseInt(strings.TrimSpace(crc32Items[1]), 10, 64)
	if err != nil {
		return 0
	}

	return crc32 % mod
}

// 保存相关数据
func (e *Executor) saveInfo(complete bool) {
	progress, progressInfo := e.getProgress(complete)

	seelog.Warnf("进度: %f, 进度信息: %s", progress, progressInfo)
}

func (e *Executor) getProgress(complete bool) (float64, string) {
	msg := fmt.Sprintf("获取数: %d, 回滚数: %d", e.ParseCount, e.ExecCount)
	if complete {
		return 100, msg
	}

	return 0, msg
}

func (e *Executor) loopSaveInfo() {
	ticker := time.NewTicker(10 * time.Second)
	for {
		select {
		case <-e.ctx.Done():
			seelog.Info("停止保存进度信息")
			ticker.Stop()
			return
		case <-ticker.C:
			e.saveInfo(false)
		}
	}
}
