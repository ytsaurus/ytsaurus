package access

import (
	"context"
	"errors"
	"fmt"

	bulkaclcheckerclient "go.ytsaurus.tech/yt/microservices/bulk_acl_checker/client_go"
	lib "go.ytsaurus.tech/yt/microservices/lib/go"
	resourceusage "go.ytsaurus.tech/yt/microservices/resource_usage/json_api_go/internal/resource_usage"
)

func (a *AccessChecker) CheckAccess(ctx context.Context, cluster string, user string, items *[]resourceusage.Item) (bool, error) {
	if a.conf.DisableACL {
		return true, nil
	}
	passContinuationToken := false
	paths := map[string]struct{}{}
	for _, item := range *items {
		path, ok := item["path"]
		if !ok {
			return passContinuationToken, errors.New("path not found")
		}
		paths[path.(string)] = struct{}{}
		parentPath := parent(path.(string))
		paths[parentPath] = struct{}{}
	}

	var proxy string
	for _, c := range a.conf.IncludedClusters {
		if c.ClusterName == cluster {
			proxy = c.Proxy
			break
		}
	}
	if proxy == "" {
		return passContinuationToken, errors.New("cluster not found")
	}

	request := &bulkaclcheckerclient.ACLCheckRequest{
		Cluster:    proxy,
		Subject:    user,
		Permission: "read",
		Paths:      lib.SetToSlice(paths),
	}

	response, err := a.aclClient.CheckACL(ctx, request)
	if err != nil {
		return passContinuationToken, err
	}

	if len(response.Actions) != len(request.Paths) {
		return passContinuationToken, fmt.Errorf("bulk_acl_checker returned %d actions for %d paths", len(response.Actions), len(request.Paths))
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

	// If the last item in Items has an empty path, it means that the user does not have access to this node.
	// Therefore, the ContinuationToken from which the path of last item can be extracted must be cleared.
	if lastItemPath, ok := item["path"].(string); ok && lastItemPath != "" {
		passContinuationToken = true
	}

	return passContinuationToken, nil
}
