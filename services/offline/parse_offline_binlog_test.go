package offline

import (
	"os"
	"testing"

	"github.com/go-mysql-org/go-mysql/replication"
)

func Test_ParseOfflineBinlog_01(t *testing.T) {
	binlogFileName := "/Users/hh/Desktop/mysql-bin.000200"

	// Create a BinlogParser object
	parser := replication.NewBinlogParser()

	err := parser.ParseFile(binlogFileName, 0, func(event *replication.BinlogEvent) error {
		event.Dump(os.Stdout)

		return nil
	})

	if err != nil {
		t.Fatal(err.Error())
	}
}
