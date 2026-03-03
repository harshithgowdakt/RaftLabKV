package raft

import (
	"fmt"
	"log"
	"os"
)

// Logger is the interface for raft logging.
type Logger interface {
	Debug(v ...interface{})
	Debugf(format string, v ...interface{})
	Info(v ...interface{})
	Infof(format string, v ...interface{})
	Warning(v ...interface{})
	Warningf(format string, v ...interface{})
	Error(v ...interface{})
	Errorf(format string, v ...interface{})
	Fatal(v ...interface{})
	Fatalf(format string, v ...interface{})
	Panic(v ...interface{})
	Panicf(format string, v ...interface{})
}

var defaultLogger = &stdLogger{log.New(os.Stderr, "raft", log.LstdFlags)}

type stdLogger struct {
	*log.Logger
}

func (l *stdLogger) Debug(v ...interface{})                 { l.Logger.Print(v...) }
func (l *stdLogger) Debugf(format string, v ...interface{}) { l.Logger.Printf(format, v...) }
func (l *stdLogger) Info(v ...interface{})                  { l.Logger.Print(v...) }
func (l *stdLogger) Infof(format string, v ...interface{})  { l.Logger.Printf(format, v...) }
func (l *stdLogger) Warning(v ...interface{})               { l.Logger.Print(v...) }
func (l *stdLogger) Warningf(format string, v ...interface{}) {
	l.Logger.Printf(format, v...)
}
func (l *stdLogger) Error(v ...interface{})                  { l.Logger.Print(v...) }
func (l *stdLogger) Errorf(format string, v ...interface{})  { l.Logger.Printf(format, v...) }
func (l *stdLogger) Fatal(v ...interface{})                  { l.Logger.Fatal(v...) }
func (l *stdLogger) Fatalf(format string, v ...interface{})  { l.Logger.Fatalf(format, v...) }
func (l *stdLogger) Panic(v ...interface{})                  { panic(fmt.Sprint(v...)) }
func (l *stdLogger) Panicf(format string, v ...interface{})  { panic(fmt.Sprintf(format, v...)) }
