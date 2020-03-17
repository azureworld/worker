package log

import (
	"log"
	"os"
)

type Logger interface {
	Println(v ...interface{})
	Printf(format string, v ...interface{})
	Print(v ...interface{})
}

var (
	logger = log.New(os.Stdout, "", 1)

	// DEBUG ...
	DEBUG Logger = logger
	// INFO ...
	INFO Logger = logger
	// WARNING ...
	WARNING Logger = logger
	// ERROR ...
	ERROR Logger = logger
	// FATAL ...
	FATAL Logger = logger
)

// Set sets a custom logger for all log levels
func Set(l *log.Logger) {
	DEBUG = l
	INFO = l
	WARNING = l
	ERROR = l
	FATAL = l
}

// SetDebug sets a custom logger for DEBUG level logs
func SetDebug(l Logger) {
	DEBUG = l
}

// SetInfo sets a custom logger for INFO level logs
func SetInfo(l Logger) {
	INFO = l
}

// SetWarning sets a custom logger for WARNING level logs
func SetWarning(l Logger) {
	WARNING = l
}

// SetError sets a custom logger for ERROR level logs
func SetError(l Logger) {
	ERROR = l
}

// SetFatal sets a custom logger for FATAL level logs
func SetFatal(l Logger) {
	FATAL = l
}
