package dlog

import (
	"log"
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
	log.Printf("%s, %d, Agent %d, "+format+"\n", time.Now().Format("2006/01/02, 15:04:05 .000"), time.Now().UnixNano(), aid, v)
}
