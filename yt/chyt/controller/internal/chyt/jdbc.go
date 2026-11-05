package chyt

import (
	"context"
	"fmt"
	"path/filepath"

	"go.ytsaurus.tech/yt/chyt/controller/internal/strawberry"
	"go.ytsaurus.tech/yt/go/ypath"
)

const (
	DefaultEnableJDBC = false

	DefaultJDBCTrampolineVersion = "jdbc-trampoline"
	DefaultJDBCBridgeJarVersion  = "clickhouse-jdbc-bridge.jar"

	JDBCTrampolineConfigFile = "jdbc-trampoline.yson"
)

type JDBCConfig struct {
	Enable            *bool        `yson:"enable"`
	Drivers           []string     `yson:"drivers"`
	ExtraFiles        []ypath.Path `yson:"extra_files"`
	TrampolineVersion *string      `yson:"trampoline_version"`
	BridgeJarVersion  *string      `yson:"bridge_jar_version"`
	JavaBin           string       `yson:"java_bin"`
	DatasourceFiles   []ypath.Path `yson:"datasource_files"`
}

func (c *JDBCConfig) EnableOrDefault() bool {
	if c == nil || c.Enable == nil {
		return DefaultEnableJDBC
	}
	return *c.Enable
}

func (c *JDBCConfig) TrampolineVersionOrDefault() string {
	if c == nil || c.TrampolineVersion == nil {
		return DefaultJDBCTrampolineVersion
	}
	return *c.TrampolineVersion
}

func (c *JDBCConfig) BridgeJarOrDefault() ypath.Path {
	version := DefaultJDBCBridgeJarVersion
	if c != nil && c.BridgeJarVersion != nil {
		version = *c.BridgeJarVersion
	}
	return JDBCDriversDirectory.Child(version)
}

type jdbcTrampolineConfig struct {
	BridgeJar       string   `yson:"bridge_jar"`
	Drivers         []string `yson:"drivers"`
	JavaBin         string   `yson:"java_bin,omitempty"`
	DatasourceFiles []string `yson:"datasource_files"`
}

func (c *Controller) buildJDBCTrampolineConfig(config *JDBCConfig) jdbcTrampolineConfig {
	dir := "."
	if c.config.LocalBinariesDir != nil {
		dir = *c.config.LocalBinariesDir
	}
	localPath := func(path ypath.Path) string {
		return filepath.Join(dir, filepath.Base(path.String()))
	}
	result := jdbcTrampolineConfig{
		BridgeJar:       localPath(config.BridgeJarOrDefault()),
		Drivers:         []string{},
		JavaBin:         config.JavaBin,
		DatasourceFiles: []string{},
	}
	for _, name := range config.Drivers {
		result.Drivers = append(result.Drivers, filepath.Join(dir, name))
	}
	for _, path := range config.DatasourceFiles {
		result.DatasourceFiles = append(result.DatasourceFiles, localPath(path))
	}
	return result
}

func (c *Controller) appendJDBCConfig(ctx context.Context, oplet *strawberry.Oplet, speclet *Speclet, filePaths *[]ypath.Rich) error {
	path, err := c.uploadYsonFile(ctx, oplet.Alias(), JDBCTrampolineConfigFile, c.buildJDBCTrampolineConfig(speclet.JDBCConfig))
	if err != nil {
		return fmt.Errorf("chyt: failed to upload JDBC trampoline config: %w", err)
	}
	*filePaths = append(*filePaths, path)
	return nil
}
