package tryt

type Config struct {
	YTAuthCookieName    *string           `yson:"yt_auth_cookie_name"`
	YTProxy             *string           `yson:"yt_proxy"`
	ExtraEnvVars        map[string]string `yson:"extra_env_vars"`
	LastActivityURLPath *string           `yson:"last_activity_url_path"`
	Command             *string           `yson:"command"`
}

const (
	DefaultYTAuthCookieName    = ""
	DefaultLastActivityURLPath = "api/status"
	DefaultCommand             = "bash -x start.sh /opt/conda/bin/jupyter lab --ip '*' --port $YT_PORT_0 --LabApp.token='' --allow-root >&2"
)

func (c *Config) YTAuthCookieNameOrDefault() string {
	if c.YTAuthCookieName != nil {
		return *c.YTAuthCookieName
	}
	return DefaultYTAuthCookieName
}

func (c *Config) LastActivityURLPathOrDefault() string {
	if c.LastActivityURLPath != nil {
		return *c.LastActivityURLPath
	}
	return DefaultLastActivityURLPath
}

func (c *Config) CommandOrDefault() string {
	if c.Command != nil {
		return *c.Command
	}
	return DefaultCommand
}
