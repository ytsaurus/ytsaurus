package main

import (
	"context"
	"fmt"
	"strings"

	"go.ytsaurus.tech/yt/go/schema"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yterrors"
)

const (
	exportTimezoneAttribute = "_access_log_viewer_export_timezone"
	// The master checks this mask on the parent to create a child; only the HTTP API of check_permission accepts it.
	childCreationPermissions = yt.Permission("write | modify_children")
)

type parentNodeAttributes struct {
	ID                             yt.NodeID `yson:"id"`
	Account                        string    `yson:"account"`
	EffectiveInheritableAttributes struct {
		PrimaryMedium string `yson:"primary_medium"`
	} `yson:"effective_inheritable_attributes"`
}

var parentNodeAttributeKeys = []string{"id", "account", "effective_inheritable_attributes"}

type outputNodeAttributes struct {
	ID             yt.NodeID     `yson:"id"`
	Type           yt.NodeType   `yson:"type"`
	Dynamic        bool          `yson:"dynamic"`
	RowCount       int64         `yson:"row_count"`
	Schema         schema.Schema `yson:"schema"`
	ExportTimezone string        `yson:"_access_log_viewer_export_timezone"`
	Account        string        `yson:"account"`
	PrimaryMedium  string        `yson:"primary_medium"`
}

var outputNodeAttributeKeys = []string{"id", "type", "dynamic", "row_count", "schema", exportTimezoneAttribute, "account", "primary_medium"}

// ensureOutput returns the static table at outputPath that the user and the service robot may use for the export,
// and whether it has been created in tx because no node existed there.
func (e *AccessLogExporter) ensureOutput(ctx context.Context, tx yt.Tx, user, robot, outputPath string) (*outputNodeAttributes, bool, error) {
	parentPath, childPath, err := ypath.Split(ypath.Path(outputPath))
	if err != nil {
		return nil, false, yterrors.Err("invalid output_path", err)
	}
	var parent parentNodeAttributes
	if err := tx.GetNode(ctx, parentPath.Attrs(), &parent, &yt.GetNodeOptions{Attributes: parentNodeAttributeKeys}); err != nil {
		return nil, false, yterrors.Err("could not read the parent directory of output_path", err)
	}
	response, err := e.operationClusterClient.CheckPermission(ctx, user, childCreationPermissions, parent.ID.YPath(), nil)
	if err != nil {
		return nil, false, yterrors.Err("could not check access to output_path", err)
	}
	if response.Action != yt.ActionAllow {
		return nil, false, yterrors.Err("write access to output_path is denied",
			yterrors.Attr("user", user),
			yterrors.Attr("permissions", []yt.Permission{yt.PermissionWrite, yt.PermissionModifyChildren}),
			yterrors.Attr("path", parentPath.String()))
	}
	// The node is resolved in the checked parent by its ID, and a link at output_path is not followed.
	nodePath := parent.ID.YPath().Child(strings.TrimPrefix(childPath, "/"))
	outputTable := &outputNodeAttributes{}
	err = tx.GetNode(ctx, nodePath.SuppressSymlink().Attrs(), outputTable, &yt.GetNodeOptions{Attributes: outputNodeAttributeKeys})
	created := false
	switch {
	case yterrors.ContainsResolveError(err):
		// The table gets the account and the medium that are checked below; without an inherited medium, the default one.
		outputTable = &outputNodeAttributes{Account: parent.Account, PrimaryMedium: parent.EffectiveInheritableAttributes.PrimaryMedium}
		if outputTable.PrimaryMedium == "" {
			outputTable.PrimaryMedium = "default"
		}
		outputTable.ID, err = tx.CreateNode(ctx, nodePath, yt.NodeTable, &yt.CreateNodeOptions{
			Attributes: map[string]any{
				"schema":                accessLogExportOutputSchema,
				"account":               outputTable.Account,
				"primary_medium":        outputTable.PrimaryMedium,
				exportTimezoneAttribute: e.config.LogsTimezone,
			},
		})
		if err != nil {
			return nil, false, yterrors.Err("could not create the output static table", err)
		}
		created = true
	case err != nil:
		return nil, false, yterrors.Err("could not read output_path attributes", err)
	case outputTable.Type != yt.NodeTable || outputTable.Dynamic:
		return nil, false, yterrors.Err("the node at output_path is not a static table",
			yterrors.Attr("type", outputTable.Type),
			yterrors.Attr("dynamic", outputTable.Dynamic))
	}
	// The access to an existing table is checked before anything about its export is returned.
	if err := e.checkOutputAccess(ctx, user, robot, outputTable, tx.ID()); err != nil {
		return nil, false, err
	}
	return outputTable, created, nil
}

func (e *AccessLogExporter) checkOutputTableForExport(outputTable *outputNodeAttributes) error {
	if outputTable.RowCount != 0 {
		return yterrors.Err("the table is not empty")
	}
	// Equal normalizes the columns of both schemas in place, so the shared export schema is copied.
	if !outputTable.Schema.Equal(accessLogExportOutputSchema.Copy()) {
		return yterrors.Err("the table schema differs from the access log export schema")
	}
	if outputTable.ExportTimezone != e.config.LogsTimezone {
		return yterrors.Err(fmt.Sprintf("the %s attribute of the table differs from the logs timezone of the service", exportTimezoneAttribute),
			yterrors.Attr("logs_timezone", e.config.LogsTimezone))
	}
	return nil
}

// checkOutputAccess checks what the master does not check before the operation starts: the write of the user and the
// robot on the output table and the use of its account and primary medium by the user.
func (e *AccessLogExporter) checkOutputAccess(ctx context.Context, user, robot string, outputTable *outputNodeAttributes, txID yt.TxID) error {
	options := &yt.CheckPermissionOptions{TransactionOptions: &yt.TransactionOptions{TransactionID: txID}}
	outputTablePath := outputTable.ID.YPath()
	response, err := e.operationClusterClient.CheckPermission(ctx, user, yt.PermissionWrite, outputTablePath, options)
	if err != nil {
		return yterrors.Err("could not check access to output_path", err)
	}
	if response.Action != yt.ActionAllow {
		return yterrors.Err("write access to output_path is denied",
			yterrors.Attr("user", user),
			yterrors.Attr("permissions", []yt.Permission{yt.PermissionWrite}),
			yterrors.Attr("path", outputTablePath.String()))
	}
	for _, resource := range []struct {
		path ypath.Path
		name string
	}{
		{ypath.Path("//sys/accounts").Child(ypath.EscapeLiteral(outputTable.Account)), "account"},
		{ypath.Path("//sys/media").Child(ypath.EscapeLiteral(outputTable.PrimaryMedium)), "primary medium"},
	} {
		response, err := e.operationClusterClient.CheckPermission(ctx, user, yt.PermissionUse, resource.path, nil)
		if err != nil {
			return yterrors.Err(fmt.Sprintf("could not check use access to the output %s", resource.name), err)
		}
		if response.Action != yt.ActionAllow {
			return yterrors.Err(fmt.Sprintf("use access to the output %s is denied", resource.name),
				yterrors.Attr("user", user),
				yterrors.Attr("permissions", []yt.Permission{yt.PermissionUse}),
				yterrors.Attr("path", resource.path.String()))
		}
	}
	response, err = e.operationClusterClient.CheckPermission(ctx, robot, yt.PermissionWrite, outputTablePath, options)
	if err != nil {
		return yterrors.Err("could not check service robot permissions for the export", err)
	}
	if response.Action != yt.ActionAllow {
		return yterrors.Err("service robot lacks permissions required for the export",
			yterrors.Attr("robot", robot),
			yterrors.Attr("permissions", []yt.Permission{yt.PermissionWrite}),
			yterrors.Attr("path", outputTablePath.String()))
	}
	return nil
}
