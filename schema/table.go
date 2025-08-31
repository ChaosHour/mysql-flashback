package schema

import (
	"fmt"
	"strings"

	"github.com/ChaosHour/mysql-flashback/dao"
	"github.com/ChaosHour/mysql-flashback/utils"
	"github.com/ChaosHour/mysql-flashback/utils/sql_parser"
	"github.com/ChaosHour/mysql-flashback/visitor"
	"github.com/cihub/seelog"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/opcode"
)

type PKType int

var (
	PKTypeAllColumns PKType = 10
	PKTypePK         PKType = 20
	PKTypeUK         PKType = 30
)

type Table struct {
	SchemaName     string
	TableName      string
	ColumnNames    []string       // 表所有的字段
	ColumnPosMap   map[string]int // 每个字段对应的slice位置
	UseColumnNames []string       // 最终需要使用的字段
	UseColumnPos   []int          // 字段对应的位点
	PKColumnNames  []string       // 主键的所有字段
	PKType                        // 主键类型 全部列. 主键. 唯一键
	InsertTemplate string         // insert sql 模板
	UpdateTemplate string         // update sql 模板
	DeleteTemplate string         // delete sql 模板
	CalcOp         []interface{}
}

func (t *Table) String() string {
	return fmt.Sprintf("%s.%s", t.SchemaName, t.TableName)
}

func NewTable(sName string, tName string) (*Table, error) {
	t := new(Table)
	t.SchemaName = sName
	t.TableName = tName

	dao := dao.NewDefaultDao()
	// 添加字段
	if err := t.addColumnNames(dao); err != nil {
		return nil, err
	}

	// 添加主键
	if err := t.addPK(dao); err != nil {
		return nil, err
	}

	// 初始化的时候将所有字段名赋值给需要的字段
	t.UseColumnNames = t.ColumnNames

	t.initAllColumnPos() // 初始化所有字段的位点信息

	t.initUseColumnPos() // 初始化使用字段的位点

	t.InitSQLTemplate()

	return t, nil
}

func NewTableWithStmt(createStmtNode *ast.CreateTableStmt) (*Table, error) {
	t := new(Table)
	t.SchemaName = createStmtNode.Table.Schema.String()
	t.TableName = createStmtNode.Table.Name.String()

	// 获取表字段
	t.ColumnNames = sql_parser.GetCreateTableColumnNames(createStmtNode)
	if len(t.ColumnNames) == 0 {
		return nil, fmt.Errorf("表: %s.%s 没有获取到字段, 请查看建表结构是否正确", t.SchemaName, t.TableName)
	}

	// 获取主键
	t.PKColumnNames, t.PKType = getUKColumnNames(createStmtNode)

	// 初始化的时候将所有字段名赋值给需要的字段
	t.UseColumnNames = t.ColumnNames

	t.initAllColumnPos() // 初始化所有字段的位点信息

	t.initUseColumnPos() // 初始化使用字段的位点

	t.InitSQLTemplate()

	return t, nil
}

func getUKColumnNames(createStmtNode *ast.CreateTableStmt) ([]string, PKType) {
	// 获取主键
	pkColumns := sql_parser.GetCreateTablePKColumnNames(createStmtNode)
	if len(pkColumns) != 0 {
		return pkColumns, PKTypePK
	}
	seelog.Warnf("表: %s.%s 没有主键", createStmtNode.Table.Schema.String(), createStmtNode.Table.Name.String())

	// 获取唯一键
	_, ukColumns := sql_parser.GetCreateTableFirstUKColumnNames(createStmtNode)
	if len(pkColumns) != 0 {
		return ukColumns, PKTypeUK
	}
	seelog.Warnf("表: %s.%s 没有唯一键", createStmtNode.Table.Schema.String(), createStmtNode.Table.Name.String())

	columnNames := sql_parser.GetCreateTableColumnNames(createStmtNode)
	return columnNames, PKTypeAllColumns
}

// 添加表的所有字段名
func (t *Table) addColumnNames(dao *dao.DefaultDao) error {
	var err error
	if t.ColumnNames, err = dao.FindTableColumnNames(t.SchemaName, t.TableName); err != nil {
		return err
	}

	if len(t.ColumnNames) == 0 {
		return fmt.Errorf("表:%s 没有获取到字段, 请确认指定表是否不存在", t.String())
	}

	return nil
}

// 添加主键
func (t *Table) addPK(dao *dao.DefaultDao) error {
	// 获取 主键
	pkColumnNames, err := dao.FindTablePKColumnNames(t.SchemaName, t.TableName)
	if err != nil {
		return fmt.Errorf("获取主键字段名出错. %v", err)
	}
	if len(pkColumnNames) > 0 {
		t.PKColumnNames = pkColumnNames
		t.PKType = PKTypePK
		return nil
	}
	seelog.Warnf("表: %s 没有主键", t.String())

	// 获取唯一键做 主键
	ukColumnNames, ukName, err := dao.FindTableUKColumnNames(t.SchemaName, t.TableName)
	if err != nil {
		return fmt.Errorf("获取唯一键做主键失败. %v", err)
	}
	if len(ukColumnNames) > 0 {
		seelog.Warnf("表: %s 设置唯一键 %s 当作主键", t.String(), ukName)
		t.PKColumnNames = ukColumnNames
		t.PKType = PKTypePK
		return nil
	}
	seelog.Warnf("表: %s 没有唯一键", t.String())

	// 所有字段为 主键
	t.PKColumnNames = t.ColumnNames
	t.PKType = PKTypeAllColumns
	seelog.Warnf("表: %s 所有字段作为该表的唯一键", t.String())

	return nil
}

// 初始所有字段的位置
func (t *Table) initAllColumnPos() {
	columnPosMap := make(map[string]int)
	for pos, name := range t.ColumnNames {
		columnPosMap[name] = pos
	}
	t.ColumnPosMap = columnPosMap
}

// 初始化使用字段位点
func (t *Table) initUseColumnPos() {
	useColumnPos := make([]int, len(t.UseColumnNames))
	for i, columnName := range t.UseColumnNames {
		pos := t.ColumnPosMap[columnName]
		useColumnPos[i] = pos
	}
	t.UseColumnPos = useColumnPos
}

// 初始化sql模板
func (t *Table) InitSQLTemplate() {
	t.initInsertTemplate()
	t.initUpdateTemplate()
	t.initDeleteTemplate()
}

// 初始化 insert sql 模板
func (t *Table) initInsertTemplate() {
	template := "/* crc32:%s, %s, threadId:%s */ INSERT INTO `%s`.`%s`(`%s`) VALUES(%s);\n"
	t.InsertTemplate = fmt.Sprintf(template, "%d", "%s", "%d", t.SchemaName, t.TableName,
		strings.Join(t.ColumnNames, "`, `"),
		utils.StrRepeat("%s", len(t.ColumnNames), ", "))
}

// 初始化 update sql 模板
func (t *Table) initUpdateTemplate() {
	template := "/* crc32:%s, %s, threadId:%s */ UPDATE `%s`.`%s` SET %s WHERE %s;\n"
	t.UpdateTemplate = fmt.Sprintf(template, "%d", "%s", "%d", t.SchemaName, t.TableName,
		utils.SqlExprPlaceholderByColumns(t.UseColumnNames, "=", "%s", ", "),
		utils.SqlExprPlaceholderByColumns(t.PKColumnNames, "=", "%s", " AND "))
}

// 初始化 delete sql 模板
func (t *Table) initDeleteTemplate() {
	template := "/* crc32:%s, %s, threadId:%s */ DELETE FROM `%s`.`%s` WHERE %s;\n"
	t.DeleteTemplate = fmt.Sprintf(template, "%d", "%s", "%d", t.SchemaName, t.TableName,
		utils.SqlExprPlaceholderByColumns(t.PKColumnNames, "=", "%s", " AND "))
}

func (t *Table) SetPKValues(row []interface{}, pkValues []interface{}) {
	for i, v := range t.PKColumnNames {
		pkValues[i] = row[t.ColumnPosMap[v]]
	}
}

// 设置 MTableInfo
func (t *Table) SetMTableInfo(mTable *visitor.MatchTable) error {
	// 设置需要的字段
	if !mTable.AllColumn {
		useColumnNames := make([]string, len(mTable.ColumnNames))
		for i, columnName := range mTable.ColumnNames {
			if _, ok := t.ColumnPosMap[columnName]; !ok {
				return fmt.Errorf("指定的字段不存在, 请确认. 库:%s, 表:%s, 字段:%s", t.SchemaName, t.TableName, columnName)
			}
			useColumnNames[i] = columnName
		}

		t.UseColumnNames = useColumnNames
		t.initUseColumnPos() // 初始化使用字段的位点
		t.InitSQLTemplate()
	}

	// 添加过滤条件
	if len(mTable.CalcOp) > 0 {
		for _, op := range mTable.CalcOp {
			switch v := op.(type) {
			case *visitor.Filter:
				colPos, ok := t.ColumnPosMap[v.Left]
				if !ok {
					return fmt.Errorf("过滤条件未匹配字段: 库:%s, 表:%s, 字段: %s", mTable.SchemaName, mTable.TableName, v.Left)
				}
				v.ColPos = colPos
			}
		}
		t.CalcOp = mTable.CalcOp
	}

	return nil
}

// 获取只用字段
func (t *Table) GetUseRow(row []interface{}) ([]interface{}, error) {
	useRow := make([]interface{}, len(t.UseColumnNames))
	for i, pos := range t.UseColumnPos {
		if pos >= len(row) {
			return useRow, fmt.Errorf("最新到表字段数 大于 binlog解析的字段数据")
		}

		useRow[i] = row[pos]
	}
	return useRow, nil
}

// Filter row
func (t *Table) FilterRow(row []interface{}) bool {
	if len(t.CalcOp) == 0 {
		return true
	}

	calc := utils.NewCalcStack()
	for _, op := range t.CalcOp {
		switch v := op.(type) {
		case *visitor.Filter:
			data := v.Compare(row[v.ColPos])
			calc.PushOrCalc(data)
		case opcode.Op:
			calc.PushOrCalc(v)
		}
	}

	if calc.IsEmpty() {
		return true
	}

	return calc.Result()
}

func (t *Table) GetPKValues(row []interface{}) []interface{} {
	pkValues := make([]interface{}, 0, len(t.PKColumnNames))
	for _, pkName := range t.PKColumnNames {
		pos := t.ColumnPosMap[pkName]
		pkValues = append(pkValues, row[pos])
	}

	return pkValues
}

func (t *Table) GetPKCrc32(row []interface{}) uint32 {
	pkValues := t.GetPKValues(row)
	return utils.GetCrc32ByInterfaceSlice(pkValues)
}
