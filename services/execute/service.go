package execute

import (
	"syscall"

	"github.com/ChaosHour/mysql-flashback/config"
	"github.com/cihub/seelog"
)

func Start(ec *config.ExecuteConfig, dbc *config.DBConfig) {
	defer seelog.Flush()
	logger, _ := seelog.LoggerFromConfigAsBytes([]byte(config.LogDefautConfig()))
	seelog.ReplaceLogger(logger)

	seelog.Infof("Rollback begins")

	if err := checkConfig(ec, dbc); err != nil {
		seelog.Error(err.Error())
		syscall.Exit(1)
	}

	config.SetExecuteConfig(ec)
	config.SetDBConfig(dbc)

	executor := NewExecutor(ec, dbc)
	if err := executor.Start(); err != nil {
		seelog.Error(err.Error())
		syscall.Exit(1)
	}
	if !executor.EmitSuccess || !executor.ExecSuccess {
		seelog.Errorf("Rollback execution failed. Executed %d statements", executor.ExecCount)
		syscall.Exit(1)
	}

	seelog.Infof("Rollback execution successful. Executed rows: %d", executor.ExecCount)
}

func checkConfig(ec *config.ExecuteConfig, _ *config.DBConfig) error {
	// Check execute subcommand configuration file, set execution type
	if err := ec.Check(); err != nil {
		return err
	}

	return nil
}
