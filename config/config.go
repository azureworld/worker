package config

const (
	// DefaultResultsExpireIn is a default time used to expire task states and group metadata from the backend
	DefaultResultsExpireIn = 3600
)

var (
	// Start with sensible default values
	defaultCnf = &Config{
		Broker:          "amqp://guest:guest@localhost:5672/",
		DefaultQueue:    "machinery_tasks",
		ResultBackend:   "amqp://guest:guest@localhost:5672/",
		ResultsExpireIn: DefaultResultsExpireIn,
	}

	//reloadDelay = time.Second * 10
)

// Config holds all configuration for our program
type Config struct {
	Broker                  string `yaml:"broker" envconfig:"BROKER"`
	MultipleBrokerSeparator string `yaml:"multiple_broker_separator" envconfig:"MULTIPLE_BROKEN_SEPARATOR"`
	DefaultQueue            string `yaml:"default_queue" envconfig:"DEFAULT_QUEUE"`
	ResultBackend           string `yaml:"result_backend" envconfig:"RESULT_BACKEND"`
	ResultsExpireIn         int    `yaml:"results_expire_in" envconfig:"RESULTS_EXPIRE_IN"`
	NoUnixSignals           bool   `yaml:"no_unix_signals" envconfig:"NO_UNIX_SIGNALS"`
}
