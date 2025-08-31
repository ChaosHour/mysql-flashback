package visitor

import (
	"fmt"
	"strings"

	"github.com/ChaosHour/mysql-flashback/utils"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/opcode"
	driver "github.com/pingcap/tidb/pkg/types/parser_driver"
)

type SelectVisitor struct {
	CurrentNodeLevel int
	TableCnt         int
	Err              error
	MTable           *MatchTable
	PosInfoColCnt    int // Count of where position fields
	EliminateOpCnt   int // Number of operators left to eliminate
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
		s.Err = fmt.Errorf("where like statement not supported")
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
	if s.TableCnt >= 1 { // already have 1 table
		return fmt.Errorf("select statement can only operate on one table")
	}
	s.TableCnt++

	tableName, ok := node.Source.(*ast.TableName)
	if !ok {
		return nil
	}
	if strings.TrimSpace(tableName.Schema.O) == "" {
		return fmt.Errorf("database name not specified")
	}
	s.MTable.TableName = tableName.Name.O
	s.MTable.SchemaName = tableName.Schema.O

	return nil
}

func (s *SelectVisitor) enterSubqueryExpr(_ *ast.SubqueryExpr) error {
	return fmt.Errorf("select statement cannot have subqueries")
}

func (s *SelectVisitor) enterBinaryOperationExpr(_ *ast.BinaryOperationExpr) error {

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
			return fmt.Errorf("where clause, field:%s, value:%v. %s", columnNameExpr.Name.Name.O, valueExpr.GetValue(), err.Error())
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
				return fmt.Errorf("sql where condition start_log_pos value cannot be converted to uint64")
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
				return fmt.Errorf("sql where condition end_log_pos value cannot be converted to uint64")
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
				return fmt.Errorf("thread_id = %#v parsing sql to get thread id error, cannot convert to int64", v)
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

// Leave select field node
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

// Leave IN clause
func (s *SelectVisitor) leavePatternInExpr(node *ast.PatternInExpr) error {
	if node.Sel != nil {
		return fmt.Errorf("in (select ...) statement not supported")
	}
	if len(node.List) == 0 {
		return fmt.Errorf("in clause value cannot be empty")
	}

	columnNameExpr, ok := node.Expr.(*ast.ColumnNameExpr)
	if !ok {
		return fmt.Errorf("failed to identify in condition field name")
	}
	// Get the type of the first element
	firstValueExpr, ok := node.List[0].(*driver.ValueExpr)
	if !ok {
		return fmt.Errorf("cannot correctly get first element in in clause")
	}
	firstValueType := GetKeyType(firstValueExpr.GetValue())

	inElement := NewInElement(firstValueType, node.Not)
	for _, nodeExpr := range node.List {
		valueExpr, ok := nodeExpr.(*driver.ValueExpr)
		if !ok {
			return fmt.Errorf("value in in clause cannot be parsed normally")
		}
		v, err := GetValueExprValue(valueExpr)
		if err != nil {
			return fmt.Errorf("value in in clause:%v. %s", valueExpr.GetValue(), err.Error())
		}
		switch firstValueType {
		case IN_KEY_TYPE_INT64:
			key, err := utils.InterfaceToInt64(v)
			if err != nil {
				return fmt.Errorf("value in in clause:%v, error converting to int64. %s", v, err.Error())
			}
			inElement.Data[key] = struct{}{}
		case IN_KEY_TYPE_UINT64:
			key, err := utils.InterfaceToUint64(v)
			if err != nil {
				return fmt.Errorf("value in in clause:%v, error converting to uint64. %s", v, err.Error())
			}
			inElement.Data[key] = struct{}{}
		case IN_KEY_TYPE_FLOAT64:
			key, err := utils.InterfaceToFloat64(v)
			if err != nil {
				return fmt.Errorf("value in in clause:%v, error converting to float64. %s", v, err.Error())
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
			return fmt.Errorf("between left and right statement, cannot correctly parse left value")
		}
		leftValue, err := GetValueExprValue(leftValueExpr)
		if err != nil {
			return fmt.Errorf("between left and right clause, field:%s, left value:%v. %s", columnNameExpr.Name.Name.O, leftValueExpr.GetValue(), err.Error())
		}

		rightValueExpr, ok := node.Right.(*driver.ValueExpr)
		if !ok {
			return fmt.Errorf("between left and right statement, cannot correctly parse right value")
		}
		rightValue, err := GetValueExprValue(rightValueExpr)
		if err != nil {
			return fmt.Errorf("between left and right clause, field:%s, right value:%v. %s", columnNameExpr.Name.Name.O, rightValueExpr.GetValue(), err.Error())
		}
		betweenAndElement := NewBetweenAndElement(node.Not, leftValue, rightValue)
		s.MTable.CalcOp = append(s.MTable.CalcOp, NewFilter(columnNameExpr.Name.Name.O, opcode.NullEQ, betweenAndElement))
	default:
		return fmt.Errorf("illegal between ... and ... clause")
	}
	return nil
}
