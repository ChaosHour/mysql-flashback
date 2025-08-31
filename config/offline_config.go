package config

import (
	"fmt"
	"strings"
	"time"

	"github.com/ChaosHour/mysql-flashback/utils"
	"github.com/cihub/seelog"
)

type OfflineConfig struct {
	BinlogFiles          []string
	ThreadID             uint32
	Now                  time.Time
	EnableRollbackUpdate bool
	EnableRollbackInsert bool
	EnableRollbackDelete bool
	SaveDir              string
	SchemaFile           string   // 建表语句
	MatchSqls            []string // 使用sql语句来匹配需要查询的时间段或者
}

func NewOffileConfig() *OfflineConfig {
	return &OfflineConfig{
		Now: time.Now(),
	}
}

// 设置最终的保存文件
func (o *OfflineConfig) GetSaveDir() string {
	if len(o.SaveDir) == 0 {
		cmdDir, err := utils.CMDDir()
		if err != nil {
			saveDir := fmt.Sprintf("./%s", SAVE_DIR)
			seelog.Errorf("获取命令所在路径失败, 使用默认路径: %s. %v",
				saveDir, err.Error())
			return saveDir
		}
		return fmt.Sprintf("%s/%s", cmdDir, SAVE_DIR)
	}

	return o.SaveDir
}

func (o *OfflineConfig) Check() error {
	if err := o.checkCondition(); err != nil {
		return err
	}

	if err := utils.CheckAndCreatePath(o.GetSaveDir(), "rollback file storage path"); err != nil {
		return err
	}

	return nil
}

func (o *OfflineConfig) checkCondition() error {
	if len(o.BinlogFiles) == 0 {
		return fmt.Errorf("please enter offline binlog file names and paths")
	}

	for _, fileName := range o.BinlogFiles {
		ok, err := utils.PathExists(fileName)
		if err != nil {
			return fmt.Errorf("error checking if offline binlog file exists, %v", err)
		}
		if !ok {
			return fmt.Errorf("offline binlog file does not exist, %v", fileName)
		}
	}

	if strings.TrimSpace(o.SchemaFile) == "" {
		return fmt.Errorf("please specify the related table structure file")
	}

	ok, err := utils.PathExists(o.SchemaFile)
	if err != nil {
		return fmt.Errorf("error checking if offline table structure file exists, %v", err)
	}
	if !ok {
		return fmt.Errorf("table structure file does not exist, %v", o.SchemaFile)
	}

	return nil
}
