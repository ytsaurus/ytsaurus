package yt

import (
	"context"
	"fmt"
	"os"

	"github.com/distribution/distribution/v3"
	"github.com/distribution/distribution/v3/manifest/ocischema"
	"github.com/distribution/distribution/v3/manifest/schema1"
	"github.com/distribution/distribution/v3/manifest/schema2"
	middleware "github.com/distribution/distribution/v3/registry/middleware/repository"
	"github.com/opencontainers/go-digest"

	"a.yandex-team.ru/yt/go/ypath"
	"a.yandex-team.ru/yt/go/yt"

	"a.yandex-team.ru/yt/go/docker_registry/internal/utils"
	auth "a.yandex-team.ru/yt/go/docker_registry/internal/ytauth"
)

const (
	middlewareName = "yt"
	ytHomeEnvName  = "DOCKER_REGISTRY_YT_HOME"
)

type Clients interface {
	Init() error
	GetClientFromContext(ctx context.Context) (yt.Client, error)
	GetClients() []yt.Client
}

type repositoryWrapper struct {
	distribution.Repository
	clients       Clients
	homeDirectory string
}

type tagServiceWrapper struct {
	distribution.TagService
	manifestService distribution.ManifestService
	clients         Clients
	repositoryName  string
	homeDirectory   string
}

func (ts *tagServiceWrapper) getCypressPathToLayerData(digest digest.Digest) ypath.Path {
	// blobDataPathSpec: <root>/docker/registry/v2/blobs/<algorithm>/<first two hex bytes of digest>/<hex digest>/data
	algorithm := digest.Algorithm()
	hex := digest.Hex()
	return ypath.Path(ts.homeDirectory + "/docker/registry/v2/blobs/" + algorithm.String() + "/" + hex[:2] + "/" + hex + "/data")
}

func (ts *tagServiceWrapper) getLayers(ctx context.Context, desc distribution.Descriptor) ([]ypath.Path, error) {
	manifest, err := ts.manifestService.Get(ctx, desc.Digest)
	if err != nil {
		return nil, err
	}

	var layers []ypath.Path
	switch manifest := manifest.(type) {
	case *schema1.SignedManifest:
		for _, fsLayer := range manifest.FSLayers {
			layers = append(layers, ts.getCypressPathToLayerData(fsLayer.BlobSum))
		}
		return layers, nil

	case *schema2.DeserializedManifest:
		for _, layerDesc := range manifest.Manifest.Layers {
			layers = append(layers, ts.getCypressPathToLayerData(layerDesc.Digest))
		}
		return layers, nil

	case *ocischema.DeserializedManifest:
		for _, layerDesc := range manifest.Manifest.Layers {
			layers = append(layers, ts.getCypressPathToLayerData(layerDesc.Digest))
		}
		return layers, nil

	default:
		return nil, fmt.Errorf("can't handle unknown manifest format")
	}
}

func (ts *tagServiceWrapper) initSchedulerHintsDocument(ctx context.Context, repositoryName string) error {
	yc, err := ts.clients.GetClientFromContext(ctx)
	if err != nil {
		return err
	}

	schedulerHintsDocument := utils.GetSchedulerHintsDocumentPath(repositoryName)
	ok, err := yc.NodeExists(ctx, schedulerHintsDocument, nil)
	if err != nil {
		return err
	}
	if ok {
		return nil
	}

	v := make(map[string]interface{})
	createNodeOptions := &yt.CreateNodeOptions{Recursive: true, Attributes: map[string]interface{}{"value": v}}
	if _, err := yc.CreateNode(ctx, schedulerHintsDocument, yt.NodeDocument, createNodeOptions); err != nil {
		return err
	}
	return nil
}

func (ts *tagServiceWrapper) getContextWithUserCredentials(ctx context.Context) (context.Context, error) {
	token, ok := auth.ReadUserTokenFromContext(ctx)
	if !ok {
		return nil, fmt.Errorf("can't read user token from context")
	}
	return yt.WithCredentials(ctx, &yt.TokenCredentials{Token: token}), nil
}

func (ts *tagServiceWrapper) saveLayersToCypress(ctx context.Context, tag string, desc distribution.Descriptor) error {
	layers, err := ts.getLayers(ctx, desc)
	if err != nil {
		return err
	}

	ctxWithUserCredentials, err := ts.getContextWithUserCredentials(ctx)
	if err != nil {
		return err
	}
	if err := ts.initSchedulerHintsDocument(ctxWithUserCredentials, ts.repositoryName); err != nil {
		return err
	}

	schedulerHintsDocument := utils.GetSchedulerHintsDocumentPath(ts.repositoryName)
	p := schedulerHintsDocument.Child(tag)
	setNodeOptions := &yt.SetNodeOptions{Recursive: true}

	yc, err := ts.clients.GetClientFromContext(ctx)
	if err != nil {
		return err
	}
	return yc.SetNode(ctxWithUserCredentials, p, layers, setNodeOptions)
}

func (ts *tagServiceWrapper) Tag(ctx context.Context, tag string, desc distribution.Descriptor) error {
	if err := ts.TagService.Tag(ctx, tag, desc); err != nil {
		return err
	}
	return ts.saveLayersToCypress(ctx, tag, desc)
}

func (ts *tagServiceWrapper) Untag(ctx context.Context, tag string) error {
	ctxWithUserCredentials, err := ts.getContextWithUserCredentials(ctx)
	if err != nil {
		return err
	}
	schedulerHintsDocument := utils.GetSchedulerHintsDocumentPath(ts.repositoryName)
	p := schedulerHintsDocument.Child(tag)

	yc, err := ts.clients.GetClientFromContext(ctx)
	if err != nil {
		return err
	}
	// FIXME: check is node exist?
	removeNodeOptions := &yt.RemoveNodeOptions{Recursive: true}
	return yc.RemoveNode(ctxWithUserCredentials, p, removeNodeOptions)
}

func (r *repositoryWrapper) Tags(ctx context.Context) distribution.TagService {
	tags := r.Repository.Tags(ctx)
	manifests, _ := r.Repository.Manifests(ctx)

	return &tagServiceWrapper{
		TagService:      tags,
		manifestService: manifests,
		clients:         r.clients,
		repositoryName:  r.Repository.Named().Name(),
		homeDirectory:   r.homeDirectory,
	}
}

func newMiddleware(ctx context.Context, repository distribution.Repository, options map[string]interface{}) (distribution.Repository, error) {
	homeDirectory := os.Getenv(ytHomeEnvName)
	if homeDirectory == "" {
		return nil, fmt.Errorf("can't find %q env variable or it's empty", ytHomeEnvName)
	}
	c := &utils.YTClients{}
	if err := c.Init(); err != nil {
		return nil, err
	}
	return &repositoryWrapper{Repository: repository, clients: c, homeDirectory: homeDirectory}, nil
}

func init() {
	_ = middleware.Register(middlewareName, newMiddleware)
}
