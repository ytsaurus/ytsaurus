package yt

import (
	"context"
	"fmt"
	"strings"

	dcontext "github.com/distribution/distribution/v3/context"
	"github.com/distribution/distribution/v3/registry/auth"

	"a.yandex-team.ru/yt/go/ypath"
	"a.yandex-team.ru/yt/go/yt"
	"a.yandex-team.ru/yt/go/yterrors"

	"a.yandex-team.ru/yt/go/docker_registry/internal/utils"
)

const (
	authName                     = "yt"
	accessResourceTypeRepository = "repository"
	accessResourceTypeRegistry   = "registry"
	accessResourceActionPush     = "push"
	accessResourceActionPull     = "pull"
	accessResourceActionDelete   = "delete"
)

type Clients interface {
	Init() error
	GetClientFromContext(ctx context.Context) (yt.Client, error)
	GetClients() []yt.Client
}

type accessController struct {
	clients Clients
}

func findExistingYTNode(ctx context.Context, c yt.Client, p ypath.YPath) (ypath.YPath, error) {
	ok, err := c.NodeExists(ctx, p, nil)
	if err != nil {
		return p, err
	}
	if ok {
		return p, nil
	}

	p, _, err = ypath.Split(p.YPath())
	if err != nil {
		return p, err
	}
	if p.YPath().String() == "/" {
		return p, nil
	}
	return findExistingYTNode(ctx, c, p)
}

func isYTWriteAuthorized(ctx context.Context, c yt.Client, userID string, p ypath.YPath) (bool, error) {
	p, err := findExistingYTNode(ctx, c, p)
	if err != nil {
		if yterrors.ContainsErrorCode(err, yterrors.CodeAuthorizationError) {
			return false, nil
		}
		return false, err
	}

	r, err := c.CheckPermission(ctx, userID, yt.PermissionWrite, p, nil)
	if err != nil {
		return false, err
	}
	if r.Action != yt.ActionAllow {
		return false, nil
	}

	var account string
	err = c.GetNode(ctx, p.YPath().Attr("account"), &account, nil)
	if err != nil {
		return false, err
	}

	ap := ypath.Root.JoinChild("sys", "accounts", account)
	r, err = c.CheckPermission(ctx, userID, yt.PermissionUse, ap, nil)
	if err != nil {
		return false, err
	}
	return r.Action == yt.ActionAllow, nil
}

func isYTActionAuthorized(ctx context.Context, c yt.Client, userID string, p ypath.YPath, permission yt.Permission) (bool, error) {
	r, err := c.CheckPermission(ctx, userID, permission, p, nil)
	if err != nil {
		// Should allow read access to non existing object due
		// docker push runs pull first
		if yterrors.ContainsErrorCode(err, yterrors.CodeResolveError) {
			return true, nil
		}
		return false, err
	}
	return r.Action == yt.ActionAllow, nil
}

func isYTReadAuthorized(ctx context.Context, c yt.Client, userID string, p ypath.YPath) (bool, error) {
	return isYTActionAuthorized(ctx, c, userID, p, yt.PermissionRead)
}

func isYTDeleteAuthorized(ctx context.Context, c yt.Client, userID string, p ypath.YPath) (bool, error) {
	return isYTActionAuthorized(ctx, c, userID, p, yt.PermissionRemove)
}

func (ac *accessController) Authorized(ctx context.Context, accessRecords ...auth.Access) (context.Context, error) {
	req, err := dcontext.GetRequest(ctx)
	if err != nil {
		return nil, &internalServerError{err}
	}

	parts := strings.Split(req.Header.Get("Authorization"), " ")
	if len(parts) != 2 {
		return nil, &authChallenge{fmt.Errorf("authentication token required")}
	}

	if parts[0] != "Basic" {
		return nil, &badRequestError{fmt.Errorf("basic authentication required")}
	}

	userToken, err := getUserToken(parts[1])
	if err != nil {
		return nil, &badRequestError{err}
	}

	userID, err := authenticateUserViaBlackBox(ctx, userToken)
	if err != nil {
		return nil, &authenticationError{err}
	}

	if len(accessRecords) == 0 {
		return enrichContextWithUserInfo(ctx, userID, userToken), nil
	}

	yc, err := ac.clients.GetClientFromContext(ctx)
	if err != nil {
		return nil, &internalServerError{err}
	}

	ctxWithUserCredentials := yt.WithCredentials(ctx, &yt.TokenCredentials{Token: userToken})

	for _, ar := range accessRecords {
		if ar.Resource.Type != accessResourceTypeRepository {
			// FIXME: ?
			continue
		}

		if ar.Action == accessResourceActionPull {
			ok, err := isYTReadAuthorized(ctxWithUserCredentials, yc, userID, utils.GetSchedulerHintsDocumentPath(ar.Resource.Name))
			if err != nil {
				return nil, &internalServerError{err}
			}
			if !ok {
				return nil, &unauthorizedError{err}
			}
		}
		if ar.Action == accessResourceActionPush {
			ok, err := isYTWriteAuthorized(ctxWithUserCredentials, yc, userID, utils.GetSchedulerHintsDocumentPath(ar.Resource.Name))
			if err != nil {
				return nil, &internalServerError{err}
			}
			if !ok {
				return nil, &unauthorizedError{err}
			}
		}
		if ar.Action == accessResourceActionDelete {
			ok, err := isYTDeleteAuthorized(ctxWithUserCredentials, yc, userID, utils.GetSchedulerHintsDocumentPath(ar.Resource.Name))
			if err != nil {
				return nil, &internalServerError{err}
			}
			if !ok {
				return nil, &unauthorizedError{err}
			}
		}
	}
	return enrichContextWithUserInfo(ctx, userID, userToken), nil
}

func newAccessController(options map[string]interface{}) (auth.AccessController, error) {
	c := &utils.YTClients{}
	if err := c.Init(); err != nil {
		return nil, err
	}
	return &accessController{clients: c}, nil
}

func init() {
	_ = auth.Register(authName, auth.InitFunc(newAccessController))
}
