package yt

import (
	"context"
	"fmt"
	"strings"

	dcontext "github.com/distribution/distribution/v3/context"
	"github.com/distribution/distribution/v3/registry/auth"

	"a.yandex-team.ru/yt/go/yt"
	"a.yandex-team.ru/yt/go/yt/ythttp"

	"a.yandex-team.ru/yt/go/docker_registry/internal/utils"
)

const (
	authName         = "yt"
	paramClusterName = "cluster"
	resourceType     = "repository"
	actionPush       = "push"
	allowAccess      = "allow"
	homeDirectory    = "home"
)

type accessController struct {
	cluster string
}

func (ac *accessController) validateYTPermissions(ctx context.Context, token string, accessRecords []auth.Access) error {
	if len(accessRecords) == 0 {
		return nil
	}

	logger, stop := utils.GetLogger()
	defer stop()

	yc, err := ythttp.NewClient(&yt.Config{Proxy: ac.cluster, Token: token, Logger: logger})
	if err != nil {
		return err
	}

	for _, ar := range accessRecords {
		if ar.Resource.Type != "repository" || ar.Action != "push" {
			continue
		}
		p := utils.GetSchedulerHintsDocumentPath(ar.Resource.Name)
		createNodeOptions := &yt.CreateNodeOptions{Recursive: true, IgnoreExisting: true}
		if _, err := yc.CreateNode(ctx, p, yt.NodeDocument, createNodeOptions); err != nil {
			return err
		}
	}
	return nil
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

	userLogin, err := authenticateUserViaBlackBox(ctx, userToken)
	if err != nil {
		return nil, &authenticationError{err}
	}

	if err := ac.validateYTPermissions(ctx, userToken, accessRecords); err != nil {
		return nil, &authenticationError{err}
	}

	return enrichContextWithUserInfo(ctx, userLogin, userToken), nil
}

func newAccessController(options map[string]interface{}) (auth.AccessController, error) {
	clusterName, ok := options[paramClusterName]
	if !ok || fmt.Sprint(clusterName) == "" {
		return nil, fmt.Errorf("no %s parameter provided", paramClusterName)
	}
	return &accessController{cluster: clusterName.(string)}, nil
}

func init() {
	_ = auth.Register(authName, auth.InitFunc(newAccessController))
}
