package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"slices"
	"strings"

	"github.com/go-chi/chi/v5"

	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yterrors"
	bac_lib "go.ytsaurus.tech/yt/microservices/bulk_acl_checker/lib_go"
	"go.ytsaurus.tech/yt/microservices/lib/go/ytmsvc"
	"go.ytsaurus.tech/yt/microservices/lib/go/ytmsvc/cypresspath"
)

const aclTreePrincipalPath = ypath.Path("//sys/access_control_object_namespaces/bulk_acl_checker/get_acl_tree/principal")

// inheritanceModeDistances tells at which distances from the node with an ACE the ACE applies:
// the node itself, its children, and the nodes two or more levels below it.
var inheritanceModeDistances = map[yt.InheritanceMode][3]bool{
	yt.InheritanceModeObjectOnly:               {true, false, false},
	yt.InheritanceModeObjectAndDescendants:     {true, true, true},
	yt.InheritanceModeDescendantsOnly:          {false, true, true},
	yt.InheritanceModeImmediateDescendantsOnly: {false, true, false},
}

// CallerAuthenticator establishes the YT user that calls Bulk ACL Checker for a cluster.
type CallerAuthenticator interface {
	AuthenticateCaller(req *http.Request, cluster string) (string, error)
}

// @Produce json
// @Param cluster path string true "Cluster"
// @Param path query string true "/ or an absolute Cypress path in the canonical YPath escaping"
// @Param username query string true "User"
// @Param permission query string false "Permission" Enums(read, write) default(read)
// @Success 200 {object} bac_lib.UserACLTreeResponse
// @Failure 400 {object} yterrors.Error
// @Router /user-acl-tree/{cluster} [get]
func createUserACLTreeHandler(callerAuthenticator CallerAuthenticator) ytmsvc.HTTPHandlerE {
	return func(w http.ResponseWriter, req *http.Request) (any, error) {
		w.Header().Set("Cache-Control", "no-store")
		query, err := url.ParseQuery(req.URL.RawQuery)
		if err != nil {
			return nil, yterrors.Err("could not parse the query string", err)
		}
		path, username := query.Get("path"), query.Get("username")
		if !cypresspath.IsCanonical(path) {
			return nil, fmt.Errorf("path %q must be / or an absolute Cypress path in the canonical YPath escaping", path)
		}
		if username == "" {
			return nil, errors.New("username is required")
		}
		permission := yt.PermissionRead
		if query.Has("permission") {
			permission = yt.Permission(query.Get("permission"))
		}
		if permission != yt.PermissionRead && permission != yt.PermissionWrite {
			return nil, fmt.Errorf("unsupported permission %q", permission)
		}
		// chi matches the escaped path when there is one, so the parameter keeps the escaping, for example %2F.
		cluster := chi.URLParam(req, "cluster")
		if req.URL.RawPath != "" {
			unescapedCluster, err := url.PathUnescape(cluster)
			if err != nil {
				return nil, yterrors.Err("could not unescape the cluster", yterrors.Attr("cluster", cluster), err)
			}
			cluster = unescapedCluster
		}
		// Check the cluster before authentication: the external build sends the caller's token to this cluster.
		clusterACLCache := Cache.Get(cluster)
		if clusterACLCache == nil {
			return nil, fmt.Errorf("unknown cluster %s", cluster)
		}
		caller, err := callerAuthenticator.AuthenticateCaller(req, cluster)
		if err != nil {
			return nil, err
		}
		if err := checkACLTreePermission(req.Context(), cluster, caller, clusterACLCache.YtClient); err != nil {
			return nil, err
		}
		return getUserACLTree(req.Context(), cluster, clusterACLCache, path, username, permission)
	}
}

// checkACLTreePermission requires use of the caller on the get_acl_tree ACO of the cluster. Decisions are cached;
// errors, including a missing ACO, are not.
func checkACLTreePermission(ctx context.Context, cluster, caller string, ytClient yt.Client) error {
	key := ACOCacheKey{Cluster: cluster, Caller: caller, Principal: aclTreePrincipalPath}
	allowed, cached := Cache.ACOLRU.Get(key)
	if !cached {
		response, err := ytClient.CheckPermission(ctx, caller, yt.PermissionUse, aclTreePrincipalPath, nil)
		if err != nil {
			return yterrors.Err("could not check the permission to get ACL trees", yterrors.Attr("caller", caller), yterrors.Attr("cluster", cluster), err)
		}
		allowed = response.Action == yt.ActionAllow
		Cache.ACOLRU.Add(key, allowed)
	}
	if !allowed {
		return yterrors.Err(yterrors.CodeAuthorizationError, "the caller has no use permission on the get_acl_tree access control object",
			yterrors.Attr("caller", caller), yterrors.Attr("cluster", cluster), yterrors.Attr("principal", aclTreePrincipalPath))
	}
	return nil
}

func getUserACLTree(ctx context.Context, cluster string, clusterACLCache *ClusterACLDump, path, username string, permission yt.Permission) (*bac_lib.UserACLTreeResponse, error) {
	if clusterACLCache.UsersExport == nil {
		return nil, fmt.Errorf("the user export of ACL snapshot %s of cluster %s is not loaded", clusterACLCache.SnapshotID, cluster)
	}
	if clusterACLCache.ACLDump.GetACL(permission) == nil {
		return nil, fmt.Errorf("ACL dump for cluster %s does not contain %s permissions", cluster, permission)
	}
	groups, ok := clusterACLCache.UsersExport[username]
	if !ok {
		return nil, fmt.Errorf("user %q is missing from ACL snapshot %s of cluster %s", username, clusterACLCache.SnapshotID, cluster)
	}
	banned, err := readUserBanned(ctx, cluster, username, clusterACLCache.YtClient)
	if err != nil {
		return nil, err
	}
	if banned {
		return nil, yterrors.Err(yterrors.CodeUserBanned, "the user is banned or no longer exists on the cluster",
			yterrors.Attr("username", username), yterrors.Attr("cluster", cluster))
	}
	response := &bac_lib.UserACLTreeResponse{Path: path, ACLSnapshotID: clusterACLCache.SnapshotID}
	if _, isSuperuser := groups["superusers"]; isSuperuser {
		response.ACLTree = &bac_lib.ACLTree{Access: &bac_lib.NodeAccess{Object: true, ImmediateDescendants: true, OtherDescendants: true}}
		return response, nil
	}
	root := clusterACLCache.ACLDump.Paths[""]
	if root == nil {
		return nil, fmt.Errorf("ACL dump %s has no Cypress root", clusterACLCache.Version)
	}
	// The dumps split a path at every "/", including an escaped one, so the requested path is split the same way.
	// TODO(ilyaibraev): Split by YPath components once Roren preprocessing does.
	var components []string
	// ["", "", "home", "dev", "test"][2:] -> ["home", "dev", "test"]
	if path != "/" {
		components = strings.Split(path, "/")[2:]
	}
	response.ACLTree = buildACLTree(root, permission, groups, components)
	return response, nil
}

// buildACLTree keeps only the branch to the requested path above it and the whole subtree below it.
func buildACLTree(aclRoot *ACLDump, permission yt.Permission, groups Groups, components []string) *bac_lib.ACLTree {
	accessRoot := &bac_lib.ACLTree{Access: nodeAccess(aclRoot, permission, groups)}
	aclNode, accessNode := aclRoot, accessRoot
	for _, component := range components {
		aclChild, ok := aclNode.Paths[component]
		if !ok {
			return accessRoot
		}
		accessChild := &bac_lib.ACLTree{Access: nodeAccess(aclChild, permission, groups)}
		accessNode.Children = map[string]*bac_lib.ACLTree{component: accessChild}
		aclNode, accessNode = aclChild, accessChild
	}
	type nodePair struct {
		aclNode    *ACLDump
		accessNode *bac_lib.ACLTree
	}
	stack := []nodePair{{aclNode: aclNode, accessNode: accessNode}}
	for len(stack) > 0 {
		pair := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if len(pair.aclNode.Paths) == 0 {
			continue
		}
		pair.accessNode.Children = make(map[string]*bac_lib.ACLTree, len(pair.aclNode.Paths))
		for name, aclChild := range pair.aclNode.Paths {
			accessChild := &bac_lib.ACLTree{Access: nodeAccess(aclChild, permission, groups)}
			pair.accessNode.Children[name] = accessChild
			stack = append(stack, nodePair{aclNode: aclChild, accessNode: accessChild})
		}
	}
	return accessRoot
}

// nodeAccess applies the ACEs of the user and their groups; deny takes precedence over allow.
func nodeAccess(aclNode *ACLDump, permission yt.Permission, groups Groups) *bac_lib.NodeAccess {
	acl := aclNode.GetACL(permission)
	if acl == nil {
		return nil
	}
	var allowed, denied [3]bool
	for index, subjects := range acl {
		action, inheritanceMode := IndexToACL(index)
		appliesToUser := slices.ContainsFunc(subjects, func(subject string) bool {
			if subject == "owner" {
				// Owners are unknown: assume the user owns the node for deny and does not for allow.
				return action == yt.ActionDeny
			}
			_, isMember := groups[subject]
			return isMember
		})
		if !appliesToUser {
			continue
		}
		for distance, applies := range inheritanceModeDistances[inheritanceMode] {
			if action == yt.ActionDeny {
				denied[distance] = denied[distance] || applies
			} else {
				allowed[distance] = allowed[distance] || applies
			}
		}
	}
	return &bac_lib.NodeAccess{
		Object:               allowed[0] && !denied[0],
		ImmediateDescendants: allowed[1] && !denied[1],
		OtherDescendants:     allowed[2] && !denied[2],
	}
}
