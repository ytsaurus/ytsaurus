// package ythttp provides YT client over HTTP protocol.
package ythttp

import (
	"io/ioutil"
	"os"
	"os/user"
	"path/filepath"
	"strings"

	"golang.org/x/xerrors"

	"a.yandex-team.ru/yt/go/mapreduce"
	"a.yandex-team.ru/yt/go/yt"
	"a.yandex-team.ru/yt/go/yt/internal/httpclient"
)

func checkNotInsideJob() error {
	if mapreduce.RequestsFromJobAllowed() {
		return nil
	}

	if mapreduce.InsideJob() {
		return xerrors.New("requests to cluster from jobs are forbidden")
	}

	return nil
}

// NewClient creates new client from config.
func NewClient(c *yt.Config) (yt.Client, error) {
	if err := checkNotInsideJob(); err != nil {
		return nil, err
	}

	return httpclient.NewHTTPClient(c)
}

// NewClientFromEnv creates YT client configured from environment variables.
//
//   YT_PROXY - required variable specifying cluster address.
//   YT_TOKEN - optional variable specifying token.
func NewClientFromEnv() (yt.Client, error) {
	c, err := yt.NewConfigFromEnv()
	if err != nil {
		return nil, err
	}

	return NewClient(c)
}

// NewClientCli creates YT client configured for use in cli application.
//
// If proxy is an empty string, YT_PROXY environment variable is used instead.
//
// Token is read from YT_TOKEN environment variable. If YT_TOKEN is not set, content of ~/.yt/token file is used instead.
func NewClientCli(proxy string) (yt.Client, error) {
	var c yt.Config

	if proxy != "" {
		c.Proxy = proxy
	} else {
		c.Proxy = os.Getenv("YT_PROXY")
		if c.Proxy == "" {
			return nil, xerrors.New("YT_PROXY environment variable is not set")
		}
	}

	var ok bool
	c.Token, ok = os.LookupEnv("YT_TOKEN")
	if !ok {
		u, err := user.Current()
		if err != nil {
			return nil, err
		}

		token, err := ioutil.ReadFile(filepath.Join(u.HomeDir, ".yt", "token"))
		if err != nil && !os.IsNotExist(err) {
			return nil, err
		}

		c.Token = strings.Trim(string(token), "\n")
	}

	return NewClient(&c)
}
