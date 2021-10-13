package logger

import (
	log "github.com/sirupsen/logrus"
)

var l *log.Logger

func Init() {
	l = log.New()

	formatter := &log.TextFormatter{
		FullTimestamp:   true,                  //日志打印时间
		TimestampFormat: "2006-01-02 15:04:05", // 定义时间戳格式
		ForceColors:     true,
	}

	l.SetFormatter(formatter)
	l.SetLevel(log.TraceLevel)

	l.Info("init logger success")
}

func ReloadLevel(confLevel string) error {
	level, err := log.ParseLevel(confLevel)
	if err != nil {
		l.Errorf("reload logger level err: %v", err)
	} else {
		l.SetLevel(level)
		l.Errorf("change log level succ: [%s]", confLevel)
	}

	return nil
}

func L() *log.Logger {
	return l
}
