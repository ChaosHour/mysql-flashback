package config

import (
	"fmt"
	"time"

	"github.com/ChaosHour/mysql-flashback/utils"
	"github.com/cihub/seelog"
)

const (
	ENABLE_ROLLBACK_UPDATE = true
	ENABLE_ROLLBACK_INSERT = true
	ENABLE_ROLLBACK_DELETE = true
	SAVE_DIR               = "rollback_sqls"
)

var sc *CreateConfig

type CreateConfig struct {
	StartLogFile         string
	StartLogPos          uint64
	EndLogFile           string
	EndLogPos            uint64
	StartTime            string
	EndTime              string
	RollbackSchemas      []string
	RollbackTables       []string
	ThreadID             uint32
	Now                  time.Time
	EnableRollbackUpdate bool
	EnableRollbackInsert bool
	EnableRollbackDelete bool
	SaveDir              string
	MatchSqls            []string // Use SQL statements to match the time period to query or
}

func NewStartConfig() *CreateConfig {
	return &CreateConfig{
		Now: time.Now(),
	}
}

func SetStartConfig(cfg *CreateConfig) {
	sc = cfg
}

func GetStartConfig() *CreateConfig {
	return sc
}

// Whether there is start position information
func (c *CreateConfig) HaveStartPosInfo() bool {
	return c.StartLogFile != ""
}

// Whether there is end position information
func (c *CreateConfig) HaveEndPosInfo() bool {
	return c.EndLogFile != ""
}

// Whether there is start event
func (c *CreateConfig) HaveStartTime() bool {
	return c.StartTime != ""
}

// Whether there is end time
func (c *CreateConfig) HaveEndTime() bool {
	return c.EndTime != ""
}

// Set the final save file
func (c *CreateConfig) GetSaveDir() string {
	if len(c.SaveDir) == 0 {
		cmdDir, err := utils.CMDDir()
		if err != nil {
			saveDir := fmt.Sprintf("./%s", SAVE_DIR)
			seelog.Errorf("Failed to get command path, using default path: %s. %v",
				saveDir, err.Error())
			return saveDir
		}
		return fmt.Sprintf("%s/%s", cmdDir, SAVE_DIR)
	}

	return c.SaveDir
}

func (c *CreateConfig) Check() error {
	if err := c.checkCondition(); err != nil {
		return err
	}

	if err := utils.CheckAndCreatePath(c.GetSaveDir(), "回滚文件存放路径"); err != nil {
		return err
	}

	return nil
}

func (c *CreateConfig) checkCondition() error {
	if c.StartLogFile != "" &&
		c.EndLogFile != "" {
		if !c.StartPosInfoLessThan(c.EndLogFile, c.EndLogPos) {
			return fmt.Errorf("specified end position is greater than start position")
		}
		return nil
	} else if c.StartLogFile != "" &&
		c.EndTime != "" {

		ts, err := utils.StrTime2Int(c.EndTime)
		if err != nil {
			return fmt.Errorf("specified end time is invalid")
		}
		if ts > (c.Now.Unix()) {
			return fmt.Errorf("specified time has not yet arrived")
		}
		return nil
	} else if c.StartTime != "" && c.EndLogFile != "" {
		return nil
	} else if c.StartTime != "" && c.EndTime != "" {
		ts, err := utils.StrTime2Int(c.EndTime)
		if err != nil {
			return fmt.Errorf("specified end time is invalid")
		}
		if ts > (c.Now.Unix()) {
			return fmt.Errorf("specified time has not yet arrived")
		}
		return nil
	}

	return fmt.Errorf("specified start and end positions are invalid")
}

// 开始位点小于其他位点
func (c *CreateConfig) StartPosInfoLessThan(otherStartFile string, otherStartPos uint64) bool {
	if c.StartLogFile < otherStartFile {
		return true
	} else if c.StartLogFile == otherStartFile {
		if c.StartLogPos < otherStartPos {
			return true
		}
	}
	return false
}

// 结束位点大于其他位点
func (c *CreateConfig) EndPostInfoRatherThan(otherEndFile string, otherEndPos uint64) bool {
	if c.EndLogFile > otherEndFile {
		return true
	} else if c.EndLogFile == otherEndFile {
		if c.EndLogPos > otherEndPos {
			return true
		}
	}
	return false
}

// 开始时间小于其他位点
func (c *CreateConfig) StartTimeLessThan(otherStartTime string) (bool, error) {
	ts1, err1 := utils.StrTime2Int(c.StartTime)
	ts2, err2 := utils.StrTime2Int(otherStartTime)
	if err1 == nil && err2 == nil {
		return ts1 < ts2, nil
	} else if err1 == nil && err2 != nil {
		return true, nil
	} else if err1 != nil && err2 == nil {
		return false, nil
	}

	return false, fmt.Errorf("error comparing start times in configuration.. %s. %s", err1.Error(), err2.Error())
}

// 结束时间大于其他位点
func (c *CreateConfig) EndTimeRatherThan(otherEndTime string) (bool, error) {
	ts1, err1 := utils.StrTime2Int(c.EndTime)
	ts2, err2 := utils.StrTime2Int(otherEndTime)
	if err1 == nil && err2 == nil {
		return ts1 > ts2, nil
	} else if err1 == nil && err2 != nil {
		return true, nil
	} else if err1 != nil && err2 == nil {
		return false, nil
	}

	return false, fmt.Errorf("error comparing end times in configuration. %s. %s", err1.Error(), err2.Error())
}

func (c *CreateConfig) StartInfoString() string {
	return fmt.Sprintf("开始位点: %s:%d. 开始时间: %s", c.StartLogFile, c.StartLogPos, c.StartTime)
}

func (c *CreateConfig) EndInfoString() string {
	return fmt.Sprintf("结束位点: %s:%d. 结束时间: %s", c.EndLogFile, c.EndLogPos, c.EndTime)
}
