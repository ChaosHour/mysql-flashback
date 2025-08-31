package offline

import (
	"fmt"
	"os"
	"strings"
	"syscall"

	"github.com/ChaosHour/mysql-flashback/config"
	"github.com/ChaosHour/mysql-flashback/schema"
	"github.com/ChaosHour/mysql-flashback/utils/sql_parser"
	"github.com/ChaosHour/mysql-flashback/visitor"
	"github.com/cihub/seelog"
)

func Start(offlineCfg *config.OfflineConfig) {
	defer seelog.Flush()
	logger, _ := seelog.LoggerFromConfigAsBytes([]byte(config.LogDefautConfig()))
	seelog.ReplaceLogger(logger)

	// Check if startup configuration information is available
	if err := offlineCfg.Check(); err != nil {
		seelog.Error(err.Error())
		syscall.Exit(1)
	}

	// 解析sql并且获取

	mTables := make([]*visitor.MatchTable, 0, len(offlineCfg.MatchSqls))
	if len(offlineCfg.MatchSqls) > 0 {
		for _, matchSql := range offlineCfg.MatchSqls {
			if strings.TrimSpace(matchSql) == "" {
				continue
			}

			tmpMTables, err := visitor.GetMatchTables(matchSql)
			if err != nil {
				seelog.Errorf(err.Error())
				syscall.Exit(1)
			}

			mTables = append(mTables, tmpMTables...)
		}
	}

	tableMap, err := getTableWithFile(offlineCfg.SchemaFile)
	if err != nil {
		seelog.Errorf("读取表结构文件构建表信息出错. %v", err.Error())
		syscall.Exit(1)
	}

	// --match-sql 中的表没有表结构报错
	for _, mTable := range mTables {
		key := fmt.Sprintf("%s.%s", mTable.SchemaName, mTable.TableName)
		if _, ok := tableMap[key]; !ok {
			seelog.Warnf("--match-sql 参数对应的表 %s, 没有找到对应到表结构信息", key)
		}
	}

	flashback, err := NewOfflineCreator(offlineCfg, mTables, tableMap)
	if err != nil {
		seelog.Error(err.Error())
		syscall.Exit(1)
	}
	if err = flashback.Start(); err != nil {
		seelog.Errorf("Failed to generate rollback SQL. %s", err.Error())
		syscall.Exit(1)
	}

	if !flashback.Successful {
		seelog.Error("Failed to generate rollback SQL")
		syscall.Exit(1)
	}
	seelog.Info("Rollback SQL generation completed")
}

// Get table information from file
func getTableWithFile(filename string) (map[string]*schema.Table, error) {
	content, err := os.ReadFile(filename)
	if err != nil {
		return nil, fmt.Errorf("error opening table statement file. File: %v. %v", filename, err)
	}
	queryStr := string(content)

	createStmtNodes, err := sql_parser.ParseCreateStmts(queryStr)
	if err != nil {
		return nil, err
	}

	tableMap := make(map[string]*schema.Table)
	for _, createStmtNode := range createStmtNodes {
		if createStmtNode.Table.Schema.String() == "" {
			return nil, fmt.Errorf("解析 CREATE TABLE 语句的表: %v 没有指定数据库, 请指定. 你可以在最开头使用 USE 语句, 代表后面的表使用同一个数据库名.", createStmtNode.Table.Name.String())
		}

		key := fmt.Sprintf("%v.%v", createStmtNode.Table.Schema.String(), createStmtNode.Table.Name.String())
		t, err := schema.NewTableWithStmt(createStmtNode)
		if err != nil {
			return nil, fmt.Errorf("构建新建表信息出错. %v", key)
		}

		tableMap[key] = t
	}

	if len(tableMap) == 0 {
		return nil, fmt.Errorf("表结构文件中没有建表语句. 文件: %v", filename)
	}

	return tableMap, nil
}
