package parse

import (
	. "github.com/wingerx/drt/tools"
	"log"
	"os"
	"strings"
)

const UTF8 = "UTF8"

type binlogFormatImage []string

func (bfi binlogFormatImage) contains(p string) bool {
	for _, v := range bfi {
		if v == strings.ToUpper(p) {
			return true
		}
	}
	return false
}

//var binlogFormat = binlogFormatImage{"STATEMENT", "ROW", "MIXED"}
//var binlogImage = binlogFormatImage{"FULL", "MINIMAL", "NOBLOB"}

// Master heartbeat interval
const MASTER_HEARTBEAT_PERIOD_SECONDS = 15

var errLog Logger = log.New(os.Stderr, "[Parse] ", log.Ldate|log.Ltime|log.Lshortfile)
