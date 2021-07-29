package dlog

import "log"

const DLOG = false

func Printf(format string, v ...interface{}) {
	if !DLOG {
		return
	}
	dlog.Printf(format, v...)
}

func Println(v ...interface{}) {
	if !DLOG {
		return
	}
	dlog.Println(v...)
}
