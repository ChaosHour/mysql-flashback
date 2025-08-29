package config

import (
	"fmt"
	"strings"
)

const (
	EXECUTE_PARALLER = 1
)

var ec *ExecuteConfig

type ExecuteConfig struct {
	FilePath string
	Paraller int64
}

func SetExecuteConfig(cfg *ExecuteConfig) {
	ec = cfg
}

func GetExecuteConfig() *ExecuteConfig {
	return ec
}

func (e *ExecuteConfig) Check() error {
	if err := e.checkCondition(); err != nil {
		return err
	}
	return nil
}

func (e *ExecuteConfig) checkCondition() error {
	if e.Paraller < 1 {
		e.Paraller = EXECUTE_PARALLER
	}

	if len(strings.TrimSpace(e.FilePath)) == 0 {
		return fmt.Errorf("请指定需要执行的文件")
	}

	return nil
}
