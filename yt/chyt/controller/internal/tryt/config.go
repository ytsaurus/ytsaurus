package tryt

type Config struct {
	ExtraEnvVars map[string]string `yson:"extra_env_vars"`
	Command      *string           `yson:"command"`
}

const (
	DefaultCommand = "/usr/local/bin/trcli replicate --transfer /usr/local/bin/transfer.yaml --log-level info --log-config minimal"
)

func (c *Config) CommandOrDefault(Speclet) string {
	if c.Command != nil {
		return *c.Command
	}
	return DefaultCommand
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
