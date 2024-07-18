package jupyt

import (
	"context"
	"encoding/json"

	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
)

type jupytServerConfig struct {
	YTProxy          string `json:"yt_proxy"`
	YTAuthCookieName string `json:"yt_auth_cookie_name"`
	YTACOName        string `json:"yt_aco_name"`
	YTACONamespace   string `json:"yt_aco_namespace"`
	YTACORootPath    string `json:"yt_aco_root_path"`
}

func (c Controller) artifactDir(alias string) ypath.Path {
	return c.root.Child(alias).Child("artifacts")
}

func (c *Controller) uploadConfig(ctx context.Context, alias string, filename string, config jupytServerConfig) (richPath ypath.Rich, err error) {
	configJSON, err := json.MarshalIndent(config, "", "    ")
	if err != nil {
		return
	}
	path := c.artifactDir(alias).Child(filename)
	_, err = c.ytc.CreateNode(ctx, path, yt.NodeFile, &yt.CreateNodeOptions{IgnoreExisting: true})
	if err != nil {
		return
	}
	w, err := c.ytc.WriteFile(ctx, path, nil)
	if err != nil {
		return
	}
	_, err = w.Write(configJSON)
	if err != nil {
		return
	}
	err = w.Close()
	if err != nil {
		return
	}
	richPath = ypath.Rich{Path: path, FileName: filename}
	return
}
