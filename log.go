package eslgo

import (
	"github.com/sirupsen/logrus"
	"io"
	"os"
	"time"
)

//Logger taken from Logrus methods
type Logger interface {
	Debugf(string, ...interface{})
	Errorf(string, ...interface{})
	Warnf(string, ...interface{})

	Debug(...interface{})
	Warn(...interface{})
	Warning(...interface{})
	Error(...interface{})
}

func NewLogger() Logger {
	logger := logrus.New()
	logger.SetFormatter(&logrus.TextFormatter{
		DisableTimestamp: false,
		FullTimestamp:    true,
		TimestampFormat:  time.RFC3339Nano,
	})
	logger.Out = io.MultiWriter(os.Stdout)
	logger.SetLevel(logLevel())
	return logger
}

func logLevel() logrus.Level {
	switch GetEnvString("ESL_LOG_LEVEL", "ERROR") {
	case "DEBUG":
		return logrus.DebugLevel
	case "WARN":
		return logrus.WarnLevel
	case "INFO":
		return logrus.InfoLevel
	case "ERROR":
		return logrus.ErrorLevel
	}
	return logrus.DebugLevel
}
