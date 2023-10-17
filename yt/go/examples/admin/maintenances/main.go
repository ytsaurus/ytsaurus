package main

import (
	"context"
	"fmt"
	"os"

	"go.ytsaurus.tech/library/go/ptr"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yt/ythttp"
	"go.ytsaurus.tech/yt/go/ytlog"
)

const (
	cluster     = "hume"
	examplePath = "yt/go/examples/admin/maintenances"
)

var logger = ytlog.Must()

func Example(yc yt.Client) {
	ctx := context.Background()

	// Get nodes
	var nodes []string
	_ = yc.ListNode(ctx, ypath.Path("//sys/tablet_nodes"), &nodes, nil)
	node := nodes[0]

	fmt.Printf("example node: %q\n", node)

	// Adding new maintenance requests
	addResp, _ := yc.AddMaintenance(ctx, yt.MaintenanceComponentClusterNode, node, yt.MaintenanceTypeDisableWriteSessions, examplePath, nil)
	_, _ = yc.AddMaintenance(ctx, yt.MaintenanceComponentClusterNode, node, yt.MaintenanceTypeDisableSchedulerJobs, examplePath+" jobs1", nil)
	_, _ = yc.AddMaintenance(ctx, yt.MaintenanceComponentClusterNode, node, yt.MaintenanceTypeDisableSchedulerJobs, examplePath+" jobs2", nil)
	_, _ = yc.AddMaintenance(ctx, yt.MaintenanceComponentClusterNode, node, yt.MaintenanceTypePendingRestart, examplePath, nil)

	// Remove disable_scheduler_jobs maintenances
	removeResp, _ := yc.RemoveMaintenance(ctx, yt.MaintenanceComponentClusterNode, node, &yt.RemoveMaintenanceOptions{
		Type: ptr.T(yt.MaintenanceTypeDisableSchedulerJobs),
		Mine: ptr.Bool(true),
	})

	fmt.Printf("Removed 2 DisableSchedulerJobs maintenances:\n%+v\n", *removeResp)

	// Remove disable_write_sessions maintenance using id
	removeResp, _ = yc.RemoveMaintenance(ctx, yt.MaintenanceComponentClusterNode, node, &yt.RemoveMaintenanceOptions{
		IDs: []yt.MaintenanceID{addResp.ID},
	})

	fmt.Printf("Removed DisableWriteSessions maintenance:\n%+v\n", *removeResp)

	removeResp, _ = yc.RemoveMaintenance(ctx, yt.MaintenanceComponentClusterNode, node, &yt.RemoveMaintenanceOptions{
		Mine: ptr.Bool(true),
	})

	fmt.Printf("Removed PendingRestart maintenance:\n%+v\n", *removeResp)
}

func main() {
	yc, err := ythttp.NewClient(&yt.Config{
		Proxy:             cluster,
		ReadTokenFromFile: true,
	})
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "error: %+v\n", err)
		os.Exit(1)
	}

	Example(yc)
}
