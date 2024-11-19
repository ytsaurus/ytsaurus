package tryt

import "fmt"

type Config struct {
	ExtraEnvVars map[string]string `yson:"extra_env_vars"`
	Command      *string           `yson:"command"`
	LogLevel     *string           `yson:"log_level"`
	LogConfig    *string           `yson:"log_config"`
	JobCount     *int              `yson:"job_count"`
}

func (c *Config) CommandOrDefault(spec Speclet) string {
	if c.Command != nil {
		return *c.Command
	}
	return fmt.Sprintf("/usr/local/bin/trcli %s --transfer /usr/local/bin/transfer.yaml --log-level %s --log-config %s", spec.Command(), c.LogLevelOrDefault(), c.LogConfigOrDefault())
}

func (c *Config) EnvVars(speclet Speclet) map[string]string {
	res := map[string]string{
		"NB_GID":        "0",
		"NB_UID":        "0",
		"NB_USER":       "root",
		"YT_BASE_LAYER": speclet.DockerImage,
	}
	for k, v := range c.ExtraEnvVars {
		res[k] = v
	}
	return res
}

func (c *Config) LogLevelOrDefault() string {
	if c.LogLevel != nil {
		return *c.LogLevel
	}
	return "info"
}

func (c *Config) LogConfigOrDefault() string {
	if c.LogConfig != nil {
		return *c.LogConfig
	}
	return "console"
}

func (c *Config) JobCountOrDefault() int {
	if c.JobCount != nil {
		return *c.JobCount
	}
	return 1
}
