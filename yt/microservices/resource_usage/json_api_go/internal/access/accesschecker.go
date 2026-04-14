package access

import (
	"context"
	"errors"
	"fmt"

	bulkaclcheckerclient "go.ytsaurus.tech/yt/microservices/bulk_acl_checker/client_go"
	"go.ytsaurus.tech/yt/microservices/lib/go/ytmsvc"
	resourceusage "go.ytsaurus.tech/yt/microservices/resource_usage/json_api_go/internal/resource_usage"
)

func (a *AccessChecker) CheckAccess(ctx context.Context, cluster string, user string, items *[]resourceusage.Item) error {
	if a.conf.DisableACL {
		return nil
	}
	paths := map[string]struct{}{}
	for _, item := range *items {
		path, ok := item["path"]
		if !ok {
			return errors.New("path not found")
		}
		paths[path.(string)] = struct{}{}
		parentPath := parent(path.(string))
		paths[parentPath] = struct{}{}
	}

	request := &bulkaclcheckerclient.ACLCheckRequest{
		Cluster:    cluster,
		Subject:    user,
		Permission: "read",
		Paths:      ytmsvc.SetToSlice(paths),
	}

	response, err := a.aclClient.CheckACL(ctx, request)
	if err != nil {
		return err
	}

	if len(response.Actions) != len(request.Paths) {
		return fmt.Errorf("bulk_acl_checker returned %d actions for %d paths", len(response.Actions), len(request.Paths))
	}

	aclMap := map[string]string{}
	for i, path := range request.Paths {
		aclMap[path] = response.Actions[i]
	}

	var item resourceusage.Item
	for _, item = range *items {
		path := item["path"].(string)
		item["acl_status"] = aclMap[path]
		if aclMap[path] != "allow" && aclMap[parent(path)] != "allow" {
			item["path"] = ""
		}
	}

	return nil
}
