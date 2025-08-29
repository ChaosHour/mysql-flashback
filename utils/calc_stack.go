package utils

import (
	"github.com/pingcap/tidb/pkg/parser/opcode"
)

type BoolNode struct {
	Data bool
	Next *BoolNode
}

func NewBoolNode(d bool) *BoolNode {
	return &BoolNode{
		Data: d,
	}
}

type CalcStack struct {
	top     *BoolNode
	result  bool
	isFirst bool
}

func NewCalcStack() *CalcStack {
	return &CalcStack{
		isFirst: true,
	}
}

func (c *CalcStack) PushOrCalc(data interface{}) {
	switch v := data.(type) {
	case opcode.Op:
		c.Calc(v)
	case bool:
		c.Push(v)
	}
}

func (c *CalcStack) Calc(op opcode.Op) {
	data, ok := c.Pop()
	if !ok {
		return
	}
	switch op {
	case opcode.LogicAnd:
		c.result = data && c.result
	case opcode.LogicOr:
		c.result = data || c.result
	}
}

func (c *CalcStack) IsEmpty() bool {
	return c.isFirst && c.top == nil
}

func (c *CalcStack) Result() bool {
	return c.result
}

func (c *CalcStack) Push(data bool) {
	if c.isFirst {
		c.result = data
		c.isFirst = false
		return
	}
	newNode := NewBoolNode(c.result)
	c.result = data
	if c.top == nil { // 空队列
		c.top = newNode
		return
	}

	// 非空队列
	tmpNode := c.top
	c.top = newNode
	newNode.Next = tmpNode
}

func (c *CalcStack) Pop() (bool, bool) {
	// 空队列
	if c.top == nil {
		return false, false
	}

	// 非空队列
	data := c.top.Data
	c.top = c.top.Next

	return data, true
}
