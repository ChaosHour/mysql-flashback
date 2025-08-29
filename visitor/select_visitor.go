package visitor

import (
	"fmt"
	"github.com/ChaosHour/mysql-flashback/utils"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/opcode"
	driver "github.com/pingcap/tidb/pkg/types/parser_driver"
	"strings"
)

type SelectVisitor struct {
	CurrentNodeLevel int
	TableCnt         int
	Err              error
	MTable           *MatchTable
	PosInfoColCnt    int // 记录 where 位点字段出现的次数
	EliminateOpCnt   int // 还有几个操作符需要消除
}

func NewSelectVisitor() *SelectVisitor {
	return &SelectVisitor{
		MTable: NewMatchTable(),
	}
}

func (s *SelectVisitor) Enter(in ast.Node) (out ast.Node, skipChildren bool) {
	s.CurrentNodeLevel++
	// fmt.Printf("%sEnter: %T, %[1]v\n", utils.StrRepeat(" ", (s.CurrentNodeLevel-1)*4, ""), in)
	if s.Err != nil {
		return in, true
	}

	switch node := in.(type) {
	case *ast.SubqueryExpr:
		s.Err = s.enterSubqueryExpr(node)
	case *ast.TableSource:
		s.Err = s.enterTableSource(node)
	case *ast.BinaryOperationExpr:
		s.Err = s.enterBinaryOperationExpr(node)
	case *ast.SelectField:
		s.Err = s.enterSelectField(node)
	case *ast.PatternLikeOrIlikeExpr:
		s.Err = fmt.Errorf("不支持 WHERE LIKE 语句")
	}

	return in, false
}

func (s *SelectVisitor) Leave(in ast.Node) (out ast.Node, ok bool) {
	defer func() {
		s.CurrentNodeLevel--
	}()
	// fmt.Printf("%sLeave: %T\n", utils.StrRepeat(" ", (s.CurrentNodeLevel-1)*4, ""), in)

	if s.Err != nil {
		return in, false
	}

	switch node := in.(type) {
	case *ast.BinaryOperationExpr:
		s.Err = s.leaveBinaryOperationExpr(node)
	case *ast.PatternInExpr:
		s.Err = s.leavePatternInExpr(node)
	case *ast.BetweenExpr:
		s.Err = s.leaveBetweenExpr(node)
	}

	return in, true
}

func (s *SelectVisitor) enterTableSource(node *ast.TableSource) error {
	if s.TableCnt >= 1 { // 已经有1个表了
		return fmt.Errorf("SELECT 语句只能操作一个表")
	}
	s.TableCnt++

	tableName, ok := node.Source.(*ast.TableName)
	if !ok {
		return nil
	}
	if strings.TrimSpace(tableName.Schema.O) == "" {
		return fmt.Errorf("没有指定 数据库名")
	}
	s.MTable.TableName = tableName.Name.O
	s.MTable.SchemaName = tableName.Schema.O

	return nil
}

func (s *SelectVisitor) enterSubqueryExpr(node *ast.SubqueryExpr) error {
	return fmt.Errorf("SELECT 语句不能有子查询")
}

func (s *SelectVisitor) enterBinaryOperationExpr(node *ast.BinaryOperationExpr) error {

	return nil
}

func (s *SelectVisitor) leaveBinaryOperationExpr(node *ast.BinaryOperationExpr) error {
	switch columnNameExpr := node.L.(type) {
	case *ast.ColumnNameExpr:
		valueExpr, ok := node.R.(*driver.ValueExpr)
		if !ok {
			return nil
		}
		v, err := GetValueExprValue(valueExpr)
		if err != nil {
			return fmt.Errorf("WHERE 子句, 字段:%s, 值:%v. %s", columnNameExpr.Name.Name.O, valueExpr.GetValue(), err.Error())
		}
		switch columnNameExpr.Name.Name.O {
		case "start_log_file":
			s.PosInfoColCnt++
			s.EliminateOpCnt++
			s.MTable.StartLogFile = utils.InterfaceToStr(v)
		case "start_log_pos":
			s.PosInfoColCnt++
			s.EliminateOpCnt++
			pos, err := utils.InterfaceToUint64(v)
			if err != nil {
				return fmt.Errorf("SQL where 条件 start_log_pos 值不能转化为 uint64")
			}
			s.MTable.StartLogPos = uint64(pos)
		case "end_log_file":
			s.PosInfoColCnt++
			s.EliminateOpCnt++
			s.MTable.EndLogFile = utils.InterfaceToStr(v)
		case "end_log_pos":
			s.PosInfoColCnt++
			s.EliminateOpCnt++
			pos, err := utils.InterfaceToUint64(v)
			if err != nil {
				return fmt.Errorf("SQL where 条件 end_log_pos 值不能转化为 uint64")
			}
			s.MTable.EndLogPos = uint64(pos)
		case "start_rollback_time":
			s.PosInfoColCnt++
			s.EliminateOpCnt++
			s.MTable.StartRollBackTime = utils.InterfaceToStr(v)
		case "end_rollback_time":
			s.PosInfoColCnt++
			s.EliminateOpCnt++
			s.MTable.EndRollBackTime = utils.InterfaceToStr(v)
		case "thread_id":
			s.PosInfoColCnt++
			s.EliminateOpCnt++
			theadId, err := utils.InterfaceToUint64(v)
			if err != nil {
				return fmt.Errorf("thread_id = %#v  解析SQL获取Thread ID 有误, 无法转化为 int64", v)
			}
			s.MTable.ThreadId = uint32(theadId)
		default:
			s.MTable.CalcOp = append(s.MTable.CalcOp, NewFilter(columnNameExpr.Name.Name.O, node.Op, v))
		}
	default:
		if s.EliminateOpCnt > 0 {
			s.EliminateOpCnt--
			return nil
		}
		switch node.Op {
		case opcode.LogicOr, opcode.LogicAnd:
			s.MTable.CalcOp = append(s.MTable.CalcOp, node.Op)
		}
	}

	return nil
}

// 离开 select field 节点
func (s *SelectVisitor) enterSelectField(node *ast.SelectField) error {
	if node.Expr == nil {
		s.MTable.AllColumn = true
		return nil
	}

	if s.MTable.AllColumn {
		return nil
	}

	columnNameExpr, ok := node.Expr.(*ast.ColumnNameExpr)
	if !ok {
		return nil
	}
	s.MTable.ColumnNames = append(s.MTable.ColumnNames, columnNameExpr.Name.Name.O)

	return nil
}

// 离开 In子句
func (s *SelectVisitor) leavePatternInExpr(node *ast.PatternInExpr) error {
	if node.Sel != nil {
		return fmt.Errorf("不支持 IN (SELECT ...) 语句")
	}
	if node.List == nil || len(node.List) == 0 {
		return fmt.Errorf("IN 子句值不能为空")
	}

	columnNameExpr, ok := node.Expr.(*ast.ColumnNameExpr)
	if !ok {
		return fmt.Errorf("识别 IN 条件字段名失败.")
	}
	// 获取第一个元素的类型
	firstValueExpr, ok := node.List[0].(*driver.ValueExpr)
	if !ok {
		return fmt.Errorf("不能正确获取 IN 子句中第一个元素")
	}
	firstValueType := GetKeyType(firstValueExpr.GetValue())

	inElement := NewInElement(firstValueType, node.Not)
	for _, nodeExpr := range node.List {
		valueExpr, ok := nodeExpr.(*driver.ValueExpr)
		if !ok {
			return fmt.Errorf("IN 子句中的值不能正常解析")
		}
		v, err := GetValueExprValue(valueExpr)
		if err != nil {
			return fmt.Errorf("IN 子句中值:%v. %s", valueExpr.GetValue(), err.Error())
		}
		switch firstValueType {
		case IN_KEY_TYPE_INT64:
			key, err := utils.InterfaceToInt64(v)
			if err != nil {
				return fmt.Errorf("IN 子句中的值:%v, 转化为 Int64 出错. %s", v, err.Error())
			}
			inElement.Data[key] = struct{}{}
		case IN_KEY_TYPE_UINT64:
			key, err := utils.InterfaceToUint64(v)
			if err != nil {
				return fmt.Errorf("IN 子句中的值:%v, 转化为 Uint64 出错. %s", v, err.Error())
			}
			inElement.Data[key] = struct{}{}
		case IN_KEY_TYPE_FLOAT64:
			key, err := utils.InterfaceToFloat64(v)
			if err != nil {
				return fmt.Errorf("IN 子句中的值:%v, 转化为 Float64 出错. %s", v, err.Error())
			}
			inElement.Data[key] = struct{}{}
		case IN_KEY_TYPE_STR:
			key := utils.InterfaceToStr(v)
			inElement.Data[key] = struct{}{}
		}
	}
	s.MTable.CalcOp = append(s.MTable.CalcOp, NewFilter(columnNameExpr.Name.Name.O, opcode.In, inElement))

	return nil
}

func (s *SelectVisitor) leaveBetweenExpr(node *ast.BetweenExpr) error {
	switch columnNameExpr := node.Expr.(type) {
	case *ast.ColumnNameExpr:
		leftValueExpr, ok := node.Left.(*driver.ValueExpr)
		if !ok {
			return fmt.Errorf("Between LEFT and RIGHT 语句, 不能正确解析 LEFT 值")
		}
		leftValue, err := GetValueExprValue(leftValueExpr)
		if err != nil {
			return fmt.Errorf("Between LEFT and RIGHT 子句, 字段:%s, LEFT值:%v. %s", columnNameExpr.Name.Name.O, leftValueExpr.GetValue(), err.Error())
		}

		rightValueExpr, ok := node.Right.(*driver.ValueExpr)
		if !ok {
			return fmt.Errorf("Between LEFT and RIGHT 语句, 不能正确解析 RIGHT 值")
		}
		rightValue, err := GetValueExprValue(rightValueExpr)
		if err != nil {
			return fmt.Errorf("Between LEFT and RIGHT 子句, 字段:%s, RIGHT值:%v. %s", columnNameExpr.Name.Name.O, rightValueExpr.GetValue(), err.Error())
		}
		betweenAndElement := NewBetweenAndElement(node.Not, leftValue, rightValue)
		s.MTable.CalcOp = append(s.MTable.CalcOp, NewFilter(columnNameExpr.Name.Name.O, opcode.NullEQ, betweenAndElement))
	default:
		return fmt.Errorf("非法 BETWEEN ... AND ... 子句")
	}
	return nil
}
