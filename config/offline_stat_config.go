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

	if err := utils.CreateDir(o.SaveDir); err != nil {
		return fmt.Errorf("failed to create statistics save directory. Directory: %v. %v", o.SaveDir, err)
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
