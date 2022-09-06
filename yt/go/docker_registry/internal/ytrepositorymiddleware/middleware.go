package yt

import (
	"context"
	"fmt"

	"github.com/distribution/distribution/v3"
	"github.com/distribution/distribution/v3/manifest/ocischema"
	"github.com/distribution/distribution/v3/manifest/schema1"
	"github.com/distribution/distribution/v3/manifest/schema2"
	middleware "github.com/distribution/distribution/v3/registry/middleware/repository"
	"github.com/opencontainers/go-digest"

	"a.yandex-team.ru/yt/go/ypath"
	"a.yandex-team.ru/yt/go/yt"
	"a.yandex-team.ru/yt/go/yt/ythttp"

	"a.yandex-team.ru/yt/go/docker_registry/internal/utils"
	auth "a.yandex-team.ru/yt/go/docker_registry/internal/ytauth"
)

const (
	middlewareName             = "yt"
	schedulerHintsDocumentName = "scheduler_hints"
	paramClusterName           = "cluster"
	paramToken                 = "token"
	paramHomeDirectory         = "home_directory"
)

type repositoryWrapper struct {
	distribution.Repository
	client        yt.Client
	homeDirectory string
}

type tagServiceWrapper struct {
	distribution.TagService
	manifestService distribution.ManifestService
	client          yt.Client
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
	schedulerHintsDocument := utils.GetSchedulerHintsDocumentPath(repositoryName)
	ok, err := ts.client.NodeExists(ctx, schedulerHintsDocument, nil)
	if err != nil {
		return err
	}

	if ok {
		return nil
	}

	v := make(map[string]interface{})
	createNodeOptions := &yt.CreateNodeOptions{Recursive: true, Attributes: map[string]interface{}{"value": v}}
	if _, err := ts.client.CreateNode(ctx, schedulerHintsDocument, yt.NodeDocument, createNodeOptions); err != nil {
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

	return ts.client.SetNode(ctxWithUserCredentials, p, layers, setNodeOptions)
}

func (ts *tagServiceWrapper) Tag(ctx context.Context, tag string, desc distribution.Descriptor) error {
	if err := ts.TagService.Tag(ctx, tag, desc); err != nil {
		return err
	}
	return ts.saveLayersToCypress(ctx, tag, desc)
}

func (r *repositoryWrapper) Tags(ctx context.Context) distribution.TagService {
	tags := r.Repository.Tags(ctx)
	manifests, _ := r.Repository.Manifests(ctx)

	return &tagServiceWrapper{
		TagService:      tags,
		manifestService: manifests,
		client:          r.client,
		repositoryName:  r.Repository.Named().Name(),
		homeDirectory:   r.homeDirectory,
	}
}

func initFunc(ctx context.Context, repository distribution.Repository, options map[string]interface{}) (distribution.Repository, error) {
	clusterName, ok := options[paramClusterName]
	if !ok || fmt.Sprint(clusterName) == "" {
		return nil, fmt.Errorf("no %s parameter provided", paramClusterName)
	}

	token, ok := options[paramToken]
	if !ok {
		token = ""
	}

	homeDirectory, ok := options[paramHomeDirectory]
	if !ok || fmt.Sprint(homeDirectory) == "" {
		return nil, fmt.Errorf("no %s parameter provided", paramHomeDirectory)
	}

	logger, stop := utils.GetLogger()
	defer stop()

	config := &yt.Config{Proxy: clusterName.(string), Logger: logger}
	if token == "" {
		config.ReadTokenFromFile = true
	} else {
		config.Token = token.(string)
	}

	yc, err := ythttp.NewClient(config)
	if err != nil {
		return nil, err
	}

	repoWrapper := &repositoryWrapper{
		Repository:    repository,
		client:        yc,
		homeDirectory: homeDirectory.(string),
	}

	return repoWrapper, nil
}

func init() {
	_ = middleware.Register(middlewareName, initFunc)
}
