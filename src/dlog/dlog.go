package dlog

import (
	"fmt"
	"log"
	"strings"
	"time"
)

const DLOG = false

func Printf(format string, v ...interface{}) {
	if !DLOG {
		return
	}
	log.Printf(format, v...)
}

func Println(v ...interface{}) {
	if !DLOG {
		return
	}
	log.Println(v...)
}

func AgentPrintfN(aid int32, format string, v ...interface{}) {
	//if !DLOG {
	//	return
	//}
	str := strings.Builder{}
	str.WriteString(fmt.Sprintf("%s, %d, Agent %d, ", time.Now().Format(".000"), time.Now().UnixNano(), aid))
	str.WriteString(fmt.Sprintf(format, v...))
	log.Printf(str.String())
}
