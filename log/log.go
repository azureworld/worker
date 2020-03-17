package log

import (
	"github.com/azureworld/worker/api"
	"log"
	"os"
)

var (
	logger = log.New(os.Stdout, "", 1)

	// DEBUG ...
	DEBUG api.Logger = logger
	// INFO ...
	INFO api.Logger = logger
	// WARNING ...
	WARNING api.Logger = logger
	// ERROR ...
	ERROR api.Logger = logger
	// FATAL ...
	FATAL api.Logger = logger
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
func SetDebug(l api.Logger) {
	DEBUG = l
}

// SetInfo sets a custom logger for INFO level logs
func SetInfo(l api.Logger) {
	INFO = l
}

// SetWarning sets a custom logger for WARNING level logs
func SetWarning(l api.Logger) {
	WARNING = l
}

// SetError sets a custom logger for ERROR level logs
func SetError(l api.Logger) {
	ERROR = l
}

// SetFatal sets a custom logger for FATAL level logs
func SetFatal(l api.Logger) {
	FATAL = l
}
