package offline_stat

import (
	"syscall"

	"github.com/ChaosHour/mysql-flashback/config"
	"github.com/cihub/seelog"
)

func Start(offlineStatCfg *config.OfflineStatConfig) {
	defer seelog.Flush()
	logger, _ := seelog.LoggerFromConfigAsBytes([]byte(config.LogDefautConfig()))
	seelog.ReplaceLogger(logger)

	// Check if startup configuration information is available
	if err := offlineStatCfg.Check(); err != nil {
		seelog.Error(err.Error())
		syscall.Exit(1)
	}

	offlineStat := NewOfflineStat(offlineStatCfg)
	if err := offlineStat.Start(); err != nil {
		seelog.Errorf("Statistics error. %v", err)
	} else {
		seelog.Info("Statistics completed")
	}
}
