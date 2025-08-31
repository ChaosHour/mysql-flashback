package config

import (
	"fmt"
	"strings"

	"github.com/ChaosHour/mysql-flashback/utils"
	"github.com/cihub/seelog"
	"github.com/daiguadaidai/peep"
	"github.com/go-mysql-org/go-mysql/replication"
)

const (
	DB_HOST                = "127.0.0.1"
	DB_PORT                = 3306
	DB_USERNAME            = "root"
	DB_PASSWORD            = "s3cr3t"
	DB_SCHEMA              = ""
	DB_AUTO_COMMIT         = true
	DB_MAX_OPEN_CONNS      = 8
	DB_MAX_IDEL_CONNS      = 8
	DB_CHARSET             = "utf8mb4"
	DB_TIMEOUT             = 10
	DB_PASSWORD_IS_DECRYPT = false
	SQL_LOG_BIN            = true
)

var dbConfig *DBConfig

type DBConfig struct {
	Username          string
	Password          string
	Database          string
	CharSet           string
	Host              string
	Timeout           int
	Port              int
	MaxOpenConns      int
	MaxIdelConns      int
	AllowOldPasswords int
	AutoCommit        bool
	PasswordIsDecrypt bool
	SqlLogBin         bool
}

func (c *DBConfig) GetDataSource() string {
	var dataSource string

	if c.SqlLogBin {
		dataSource = fmt.Sprintf(
			"%v:%v@tcp(%v:%v)/%v?charset=%v&allowOldPasswords=%v&timeout=%vs&autocommit=%v&parseTime=True&loc=Local",
			c.Username,
			c.GetPassword(),
			c.Host,
			c.Port,
			c.Database,
			c.CharSet,
			c.AllowOldPasswords,
			c.Timeout,
			c.AutoCommit,
		)
	} else {
		dataSource = fmt.Sprintf(
			"%v:%v@tcp(%v:%v)/%v?charset=%v&allowOldPasswords=%v&timeout=%vs&autocommit=%v&parseTime=True&loc=Local&sql_log_bin=%v",
			c.Username,
			c.GetPassword(),
			c.Host,
			c.Port,
			c.Database,
			c.CharSet,
			c.AllowOldPasswords,
			c.Timeout,
			c.AutoCommit,
			c.SqlLogBin,
		)
	}

	return dataSource
}

func (c *DBConfig) Check() error {
	if strings.TrimSpace(c.Database) == "" {
		return fmt.Errorf("数据库不能为空")
	}

	return nil
}

// 设置 DBConfig
func SetDBConfig(dbc *DBConfig) {
	dbConfig = dbc
}

func GetDBConfig() *DBConfig {
	return dbConfig
}

func (c *DBConfig) GetSyncerConfig() replication.BinlogSyncerConfig {
	return replication.BinlogSyncerConfig{
		ServerID: utils.RandRangeUint32(100000000, 200000000),
		Flavor:   "mysql",
		Host:     c.Host,
		Port:     uint16(c.Port),
		User:     c.Username,
		Password: c.GetPassword(),
	}
}

// 获取密码, 有判断是否需要进行解密
func (c *DBConfig) GetPassword() string {
	if c.PasswordIsDecrypt {
		pwd, err := peep.Decrypt(c.Password)
		if err != nil {
			seelog.Warnf("密码解密出错, 将使用未解析串作为密码. %s", err.Error())
			return c.Password
		}
		return pwd
	}
	return c.Password
}
