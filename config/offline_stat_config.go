package config

import (
	"fmt"
	"github.com/ChaosHour/mysql-flashback/utils"
)

const DefaultOfflineStatSaveDir = "offline_stat_output"

type OfflineStatConfig struct {
	SaveDir     string
	BinlogFiles []string
}

func (o *OfflineStatConfig) Check() error {
	if len(o.BinlogFiles) == 0 {
		return fmt.Errorf("请输入离线 binlog 文件名以及路径")
	}

	for _, fileName := range o.BinlogFiles {
		ok, err := utils.PathExists(fileName)
		if err != nil {
			return fmt.Errorf("检测离线 binlog 文件是否存在出错, %v", err)
		}
		if !ok {
			return fmt.Errorf("离线 binlog 文件不存在, %v", fileName)
		}
	}

	if err := utils.CreateDir(o.SaveDir); err != nil {
		return fmt.Errorf("创建统计信息保存目录出错. 目录: %v. %v", o.SaveDir, err)
	}

	return nil
}

func (o *OfflineStatConfig) TableStatFilePath() string {
	return fmt.Sprintf("%v/table_stat.txt", o.SaveDir)
}

func (o *OfflineStatConfig) ThreadStatFilePath() string {
	return fmt.Sprintf("%v/thread_stat.txt", o.SaveDir)
}

func (o *OfflineStatConfig) TimestampStatFilePath() string {
	return fmt.Sprintf("%v/timestamp_stat.txt", o.SaveDir)
}

func (o *OfflineStatConfig) TransactionStatFilePath() string {
	return fmt.Sprintf("%v/xid_stat.txt", o.SaveDir)
}
