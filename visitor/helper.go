package visitor

import (
	"fmt"

	"github.com/pingcap/tidb/pkg/types"
	driver "github.com/pingcap/tidb/pkg/types/parser_driver"
)

func GetValueExprValue(node *driver.ValueExpr) (interface{}, error) {
	value := node.GetValue()
	switch data := value.(type) {
	case *types.MyDecimal:
		v, err := data.ToFloat64()
		if err != nil {
			return nil, fmt.Errorf("type:*types.MyDecimal. error converting to Float64. %s", err.Error())
		}
		return v, nil
	}
	return value, nil
}
