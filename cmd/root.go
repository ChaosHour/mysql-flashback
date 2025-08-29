// Copyright © 2018 NAME HERE <EMAIL ADDRESS>
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cmd

import (
	"fmt"
	"os"

	"github.com/ChaosHour/mysql-flashback/services/offline"
	"github.com/ChaosHour/mysql-flashback/services/offline_stat"

	"github.com/ChaosHour/mysql-flashback/config"
	"github.com/ChaosHour/mysql-flashback/services/create"
	"github.com/ChaosHour/mysql-flashback/services/execute"
	"github.com/spf13/cobra"
)

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "mysql-flashback",
	Short: "MySQL flashback tool",
}

// createCmd is a subcommand of rootCmd
var createCmd = &cobra.Command{
	Use:   "create",
	Short: "Generate rollback SQL",
	Long: `Generate rollback SQL. As follows:
Example:
Specify start position and end position
./mysql-flashback create \
    --start-log-file="mysql-bin.000090" \
    --start-log-pos=0 \
    --end-log-file="mysql-bin.000092" \
    --end-log-pos=424 \
    --thread-id=15 \
    --rollback-table="schema1.table1" \
    --rollback-table="schema1.table2" \
    --rollback-table="schema2.table1" \
    --save-dir="" \
    --db-host="127.0.0.1" \
    --db-port=3306 \
    --db-username="root" \
    --db-password="root" \
    --match-sql="select * from schema1.table1 where name = 'aa'"

Specify start position and end time
./mysql-flashback create \
    --start-log-file="mysql-bin.000090" \
    --start-log-pos=0 \
    --end-time="2018-12-17 15:36:58" \
    --thread-id=15 \
    --rollback-table="schema1.table1" \
    --rollback-table="schema1.table2" \
    --rollback-table="schema2.table1" \
    --save-dir="" \
    --db-host="127.0.0.1" \
    --db-port=3306 \
    --db-username="root" \
    --db-password="root" \
    --match-sql="select name, age from schema1.table1 where name = 'aa'"

Specify start time and end time
./mysql-flashback create \
    --start-time="2018-12-14 15:00:00" \
    --end-time="2018-12-17 15:36:58" \
    --thread-id=15 \
    --rollback-schema="schema1" \
    --rollback-table="table1" \
    --rollback-table="schema1.table2" \
    --rollback-table="schema2.table1" \
    --save-dir="" \
    --db-host="127.0.0.1" \
    --db-port=3306 \
    --db-username="root" \
    --db-password="root" \
    --match-sql="select name, age from schema1.table1 where name = 'aa' and age = 2"
`,
	Run: func(cmd *cobra.Command, args []string) {
		create.Start(cc, cdbc)
	},
}

// offlineCmd is a subcommand of rootCmd
var offlineCmd = &cobra.Command{
	Use:   "offline",
	Short: "Parse offline binlog, generate rollback SQL",
	Long: `Parse offline binlog, generate rollback SQL. As follows:
Example:
./mysql-flashback offline \
    --enable-rollback-insert=true \
    --enable-rollback-update=true \
    --enable-rollback-delete=true \
    --thread-id=15 \
    --save-dir="" \
    --schema-file="" \
    --match-sql="select * from schema1.table1 where name = 'aa'" \
    --match-sql="select * from schema2.table1 where name = 'aa'" \
    --binlog-file="mysql-bin.0000001" \
    --binlog-file="mysql-bin.0000002"
`,
	Run: func(cmd *cobra.Command, args []string) {
		offline.Start(offlineCfg)
	},
}

// executeCmd is a subcommand of rootCmd
var executeCmd = &cobra.Command{
	Use:   "execute",
	Short: "Execute SQL rollback file",
	Long: `Execute the specified SQL rollback file in reverse order. As follows:
Example:
./mysql-flashback execute \
    --filepath="/tmp/test.sql" \
    --paraller=8 \
    --sql-log-bin=true \
    --db-host="127.0.0.1" \
    --db-port=3306 \
    --db-username="root" \
    --db-password="root"
`,
	Run: func(cmd *cobra.Command, args []string) {
		execute.Start(ec, edbc)
	},
}

// offlineStatCmd is a subcommand of rootCmd
var offlineStatCmd = &cobra.Command{
	Use:   "offline-stat",
	Short: "Parse offline binlog, generate binlog statistics",
	Long: `Parse offline binlog, generate binlog statistics. As follows:
After successful execution, 4 files will be generated in the current directory
offline_stat_output/table_stat.txt # Save table statistics
offline_stat_output/thread_stat.txt # Save thread statistics
offline_stat_output/timestamp_stat.txt # Save time statistics (records the time when each transaction executes BEGIN)
offline_stat_output/xid_stat.txt # Save xid statistics

Example:
./mysql-flashback offline-stat \
    --save-dir="offline_stat_output" \
    --binlog-file="mysql-bin.0000001" \
    --binlog-file="mysql-bin.0000002"
`,
	Run: func(cmd *cobra.Command, args []string) {
		offline_stat.Start(offlineStatCfg)
	},
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func init() {
	addCreateCMD()
	addExecuteCMD()
	addOfflineCMD()
	addOfflineStatCMD()
}

var cc *config.CreateConfig
var cdbc *config.DBConfig

// Add create rollback SQL subcommand
func addCreateCMD() {
	rootCmd.AddCommand(createCmd)
	cc = config.NewStartConfig()
	createCmd.PersistentFlags().StringVar(&cc.StartLogFile, "start-log-file", "", "Start log file")
	createCmd.PersistentFlags().Uint64Var(&cc.StartLogPos, "start-log-pos", 0, "Start log file position")
	createCmd.PersistentFlags().StringVar(&cc.EndLogFile, "end-log-file", "", "End log file")
	createCmd.PersistentFlags().Uint64Var(&cc.EndLogPos, "end-log-pos", 0, "End log file position")
	createCmd.PersistentFlags().StringVar(&cc.StartTime, "start-time", "", "Start time")
	createCmd.PersistentFlags().StringVar(&cc.EndTime, "end-time", "", "End time")
	createCmd.PersistentFlags().StringArrayVar(&cc.RollbackSchemas, "rollback-schema", []string{}, "Specify databases to rollback, this command can specify multiple")
	createCmd.PersistentFlags().StringArrayVar(&cc.RollbackTables, "rollback-table", []string{}, "Tables to rollback, this command can specify multiple")
	createCmd.PersistentFlags().Uint32Var(&cc.ThreadID, "thread-id", 0, "Thread id to rollback")
	createCmd.PersistentFlags().BoolVar(&cc.EnableRollbackInsert, "enable-rollback-insert", config.ENABLE_ROLLBACK_INSERT, "Whether to enable rollback insert")
	createCmd.PersistentFlags().BoolVar(&cc.EnableRollbackUpdate, "enable-rollback-update", config.ENABLE_ROLLBACK_UPDATE, "Whether to enable rollback update")
	createCmd.PersistentFlags().BoolVar(&cc.EnableRollbackDelete, "enable-rollback-delete", config.ENABLE_ROLLBACK_DELETE, "Whether to enable rollback delete")
	createCmd.PersistentFlags().StringVar(&cc.SaveDir, "save-dir", "", "Path to save related files")
	createCmd.PersistentFlags().StringArrayVar(&cc.MatchSqls, "match-sql", []string{}, "Use simple SELECT statement to match needed fields and records")

	cdbc = new(config.DBConfig)
	// Database connection configuration
	createCmd.PersistentFlags().StringVar(&cdbc.Host, "db-host", config.DB_HOST, "Database host")
	createCmd.PersistentFlags().IntVar(&cdbc.Port, "db-port", config.DB_PORT, "Database port")
	createCmd.PersistentFlags().StringVar(&cdbc.Username, "db-username", config.DB_USERNAME, "Database username")
	createCmd.PersistentFlags().StringVar(&cdbc.Password, "db-password", config.DB_PASSWORD, "Database password")
	createCmd.PersistentFlags().StringVar(&cdbc.Database, "db-schema", config.DB_SCHEMA, "Database name")
	createCmd.PersistentFlags().StringVar(&cdbc.CharSet, "db-charset", config.DB_CHARSET, "Database charset")
	createCmd.PersistentFlags().IntVar(&cdbc.Timeout, "db-timeout", config.DB_TIMEOUT, "Database timeout")
	createCmd.PersistentFlags().IntVar(&cdbc.MaxIdelConns, "db-max-idel-conns", config.DB_MAX_IDEL_CONNS, "Database max idle connections")
	createCmd.PersistentFlags().IntVar(&cdbc.MaxOpenConns, "db-max-open-conns", config.DB_MAX_OPEN_CONNS, "Database max open connections")
	createCmd.PersistentFlags().BoolVar(&cdbc.AutoCommit, "db-auto-commit", config.DB_AUTO_COMMIT, "Database auto commit")
	createCmd.PersistentFlags().BoolVar(&cdbc.PasswordIsDecrypt, "db-password-is-decrypt", config.DB_PASSWORD_IS_DECRYPT, "Whether database password needs decryption")
	createCmd.PersistentFlags().BoolVar(&cdbc.SqlLogBin, "sql-log-bin", config.SQL_LOG_BIN, "Whether executing SQL records binlog")
}

var offlineCfg *config.OfflineConfig

// Add offline create rollback SQL subcommand
func addOfflineCMD() {
	rootCmd.AddCommand(offlineCmd)

	offlineCfg = config.NewOffileConfig()
	offlineCmd.PersistentFlags().Uint32Var(&offlineCfg.ThreadID, "thread-id", 0, "Thread id to rollback")
	offlineCmd.PersistentFlags().BoolVar(&offlineCfg.EnableRollbackInsert, "enable-rollback-insert", config.ENABLE_ROLLBACK_INSERT, "Whether to enable rollback insert")
	offlineCmd.PersistentFlags().BoolVar(&offlineCfg.EnableRollbackUpdate, "enable-rollback-update", config.ENABLE_ROLLBACK_UPDATE, "Whether to enable rollback update")
	offlineCmd.PersistentFlags().BoolVar(&offlineCfg.EnableRollbackDelete, "enable-rollback-delete", config.ENABLE_ROLLBACK_DELETE, "Whether to enable rollback delete")
	offlineCmd.PersistentFlags().StringVar(&offlineCfg.SaveDir, "save-dir", "", "Path to save related files")
	offlineCmd.PersistentFlags().StringVar(&offlineCfg.SchemaFile, "schema-file", "", "Table structure file")
	offlineCmd.PersistentFlags().StringArrayVar(&offlineCfg.MatchSqls, "match-sql", []string{}, "Use simple SELECT statement to match needed fields and records")
	offlineCmd.PersistentFlags().StringArrayVar(&offlineCfg.BinlogFiles, "binlog-file", []string{}, "Which binlog files")
}

// Add create rollback SQL subcommand
var ec *config.ExecuteConfig
var edbc *config.DBConfig

func addExecuteCMD() {
	rootCmd.AddCommand(executeCmd)

	ec = new(config.ExecuteConfig)
	executeCmd.PersistentFlags().StringVar(&ec.FilePath, "filepath", "", "Specified file to execute")
	executeCmd.PersistentFlags().Int64Var(&ec.Paraller, "paraller", config.EXECUTE_PARALLER, "Number of rollback parallels")

	edbc = new(config.DBConfig)
	// Database connection configuration
	executeCmd.PersistentFlags().StringVar(&edbc.Host, "db-host", "", "Database host")
	executeCmd.PersistentFlags().IntVar(&edbc.Port, "db-port", -1, "Database port")
	executeCmd.PersistentFlags().StringVar(&edbc.Username, "db-username", "", "Database username")
	executeCmd.PersistentFlags().StringVar(&edbc.Password, "db-password", "", "Database password")
	executeCmd.PersistentFlags().StringVar(&edbc.Database, "db-schema", "", "Database name")
	executeCmd.PersistentFlags().StringVar(&edbc.CharSet, "db-charset", config.DB_CHARSET, "Database charset")
	executeCmd.PersistentFlags().IntVar(&edbc.Timeout, "db-timeout", config.DB_TIMEOUT, "Database timeout")
	executeCmd.PersistentFlags().IntVar(&edbc.MaxIdelConns, "db-max-idel-conns", config.DB_MAX_IDEL_CONNS, "Database max idle connections")
	executeCmd.PersistentFlags().IntVar(&edbc.MaxOpenConns, "db-max-open-conns", config.DB_MAX_OPEN_CONNS, "Database max open connections")
	executeCmd.PersistentFlags().BoolVar(&edbc.AutoCommit, "db-auto-commit", config.DB_AUTO_COMMIT, "Database auto commit")
	executeCmd.PersistentFlags().BoolVar(&edbc.PasswordIsDecrypt, "db-password-is-decrypt", config.DB_PASSWORD_IS_DECRYPT, "Whether database password needs decryption")
	executeCmd.PersistentFlags().BoolVar(&cdbc.SqlLogBin, "sql-log-bin", config.SQL_LOG_BIN, "Whether executing SQL records binlog")
}

var offlineStatCfg *config.OfflineStatConfig

// Add offline create rollback SQL subcommand
func addOfflineStatCMD() {
	rootCmd.AddCommand(offlineStatCmd)

	offlineStatCfg = new(config.OfflineStatConfig)
	offlineStatCmd.PersistentFlags().StringArrayVar(&offlineStatCfg.BinlogFiles, "binlog-file", []string{}, "Which binlog files")
	offlineStatCmd.PersistentFlags().StringVar(&offlineStatCfg.SaveDir, "save-dir", config.DefaultOfflineStatSaveDir, "Statistics save directory")
}
