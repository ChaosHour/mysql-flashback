package models

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
)

type Position struct {
	File              string    `gorm:"column:File"`
	Position          uint64    `gorm:"column:Position"`
	Binlog_Do_DB      string    `gorm:"column:Binlog_Do_DB"`
	Binlog_Ignore_DB  string    `gorm:"column:Binlog_Ignore_DB"`
	Executed_Gtid_Set string    `gorm:"column:Executed_Gtid_Set"`
	TS                time.Time `gorm:"-"`
}

func (p *Position) String() string {
	return fmt.Sprintf("%s:%d", p.File, p.Position)
}

// 比较两个位点是否一样
func (p *Position) Equal(other *Position) bool {
	if p.File != other.File {
		return false
	}
	if p.Position != other.Position {
		return false
	}

	return true
}

// 比较两个位点是否一样
func (p *Position) LessThan(other *Position) bool {
	if p.File < other.File {
		return true
	} else if p.File == other.File {
		if p.Position < other.Position {
			return true
		}
		return false
	} else {
		return false
	}
}

func (p *Position) GetFileNum() int64 {
	items := strings.Split(p.File, ".")
	numStr := items[len(items)-1]
	num, err := strconv.ParseInt(numStr, 10, 64)
	if err != nil {
		return 0
	}
	return num
}

func (p *Position) GetTotalNum() int64 {
	return (p.GetFileNum() * int64(math.MaxUint32)) + int64(p.Position)
}
