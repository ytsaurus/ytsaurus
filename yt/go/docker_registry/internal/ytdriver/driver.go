package yt

import (
	"context"
	"fmt"
	"strings"

	"io"
	"io/ioutil"

	"a.yandex-team.ru/yt/go/ypath"
	"a.yandex-team.ru/yt/go/yt"
	"a.yandex-team.ru/yt/go/yt/ythttp"
	"a.yandex-team.ru/yt/go/yterrors"

	storagedriver "github.com/distribution/distribution/v3/registry/storage/driver"
	"github.com/distribution/distribution/v3/registry/storage/driver/base"
	"github.com/distribution/distribution/v3/registry/storage/driver/factory"
)

const (
	driverName         = "yt"
	paramClusterName   = "cluster"
	paramToken         = "token"
	paramHomeDirectory = "home_directory"
)

type driver struct {
	client        yt.Client
	homeDirectory string
}

type baseEmbed struct{ base.Base }

type Driver struct{ baseEmbed }

func init() {
	factory.Register(driverName, &YTDriverFactory{})
}

type YTDriverFactory struct{}

func (factory *YTDriverFactory) Create(parameters map[string]interface{}) (storagedriver.StorageDriver, error) {
	return FromParameters(parameters)
}

// FromParameters constructs a new Driver with a given parameters map.
func FromParameters(parameters map[string]interface{}) (*Driver, error) {
	clusterName, ok := parameters[paramClusterName]
	if !ok || fmt.Sprint(clusterName) == "" {
		return nil, fmt.Errorf("no %s parameter provided", paramClusterName)
	}

	token, ok := parameters[paramToken]
	if !ok {
		token = ""
	}

	homeDirectory, ok := parameters[paramHomeDirectory]
	if !ok || fmt.Sprint(homeDirectory) == "" {
		return nil, fmt.Errorf("no %s parameter provided", paramHomeDirectory)
	}

	return New(fmt.Sprint(clusterName), fmt.Sprint(token), fmt.Sprint(homeDirectory))
}

func New(clusterName, token string, homeDirectory string) (*Driver, error) {
	config := &yt.Config{Proxy: clusterName}
	if token == "" {
		config.ReadTokenFromFile = true
	} else {
		config.Token = token
	}

	yc, err := ythttp.NewClient(config)
	if err != nil {
		return nil, err

	}
	d := &driver{client: yc, homeDirectory: homeDirectory}
	if err := d.initDriver(); err != nil {
		return nil, err
	}
	return &Driver{baseEmbed: baseEmbed{Base: base.Base{StorageDriver: d}}}, nil
}

func (d *driver) initDriver() error {
	ctx := context.Background()
	p := d.getCypressPath("")

	ok, err := d.client.NodeExists(ctx, p, nil)
	if err != nil {
		return err
	}
	if !ok {
		_, err := d.client.CreateNode(ctx, p, yt.NodeMap, &yt.CreateNodeOptions{Recursive: true})
		if err != nil {
			return err
		}
	}
	return nil
}

func (d *driver) Name() string {
	return driverName
}

func (d *driver) getCypressPath(path string) ypath.Path {
	path = d.homeDirectory + "/" + path
	p := ypath.Root
	for _, token := range strings.Split(path, "/") {
		if token == "" {
			continue
		}
		p = p.Child(token)
	}
	return p
}

func (d *driver) normalizeOutputPath(p ypath.Path) string {
	path := p.String()
	if d.homeDirectory != "" && strings.HasPrefix(path, d.homeDirectory) {
		return path[len(d.homeDirectory):]
	}
	if strings.HasPrefix(path, "//") {
		return path[1:]
	}
	return path
}

func (d *driver) translatePathResolveError(err error, p ypath.Path) error {
	if err != nil && yterrors.ContainsErrorCode(err, yterrors.CodeResolveError) {
		return storagedriver.PathNotFoundError{Path: d.normalizeOutputPath(p)}
	}
	return err
}

func (d *driver) nodeExists(ctx context.Context, p ypath.Path) error {
	ok, err := d.client.NodeExists(ctx, p, nil)
	if err != nil {
		return err
	}
	if !ok {
		return storagedriver.PathNotFoundError{Path: d.normalizeOutputPath(p)}
	}
	return nil
}

func (d *driver) GetContent(ctx context.Context, path string) ([]byte, error) {
	p := d.getCypressPath(path)

	r, err := d.client.ReadFile(ctx, p, nil)
	if err != nil {
		return nil, d.translatePathResolveError(err, p)
	}
	defer r.Close()

	data, err := ioutil.ReadAll(r)
	if err != nil {
		return nil, err
	}

	return data, nil
}

func (d *driver) Stat(ctx context.Context, path string) (storagedriver.FileInfo, error) {
	p := d.getCypressPath(path)

	var attrs statAttrs
	if err := d.client.GetNode(ctx, p.Attrs(), &attrs, nil); err != nil {
		return nil, d.translatePathResolveError(err, p)
	}

	return fileInfo{
		path: d.normalizeOutputPath(p),
		stat: attrs,
	}, nil
}

func (d *driver) PutContent(ctx context.Context, path string, content []byte) error {
	p := d.getCypressPath(path)

	// FIXME: What if it exists and it is not a NodeFile type?
	ok, err := d.client.NodeExists(ctx, p, nil)
	if err != nil {
		return err
	}
	if !ok {
		_, err := d.client.CreateNode(ctx, p, yt.NodeFile, &yt.CreateNodeOptions{Recursive: true})
		if err != nil {
			return err
		}
	}

	w, err := d.client.WriteFile(ctx, p, nil)
	if err != nil {
		return err
	}
	defer w.Close()

	_, err = w.Write(content)
	if err != nil {
		return err
	}

	if err := w.Close(); err != nil {
		return err
	}

	return nil
}

func (d *driver) Reader(ctx context.Context, path string, offset int64) (io.ReadCloser, error) {
	p := d.getCypressPath(path)
	if err := d.nodeExists(ctx, p); err != nil {
		return nil, err
	}

	readFileOptions := &yt.ReadFileOptions{Offset: &offset}
	r, err := d.client.ReadFile(ctx, p, readFileOptions)
	if err != nil {
		return nil, d.translatePathResolveError(err, p)
	}

	return r, nil
}

func (d *driver) Writer(ctx context.Context, path string, append bool) (storagedriver.FileWriter, error) {
	// FIXME: What if it exists and it is not a NodeFile type?
	p := ypath.NewRich(d.getCypressPath(path).String())
	size := int64(0)

	tx, err := d.client.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}

	ok, err := tx.NodeExists(ctx, p, nil)
	if err != nil {
		return nil, err
	}

	if !ok && append {
		return nil, fmt.Errorf("can't append to non existing node")
	}
	if !ok {
		_, err := tx.CreateNode(ctx, p, yt.NodeFile, &yt.CreateNodeOptions{Recursive: true})
		if err != nil {
			return nil, err
		}
	}

	if append {
		var attrs statAttrs
		p = p.SetAppend()
		if err := tx.GetNode(ctx, d.getCypressPath(path).Attrs(), &attrs, nil); err != nil {
			return nil, err
		}
		size = attrs.UncompressedDataSize
	}

	w, err := tx.WriteFile(ctx, p, nil)
	if err != nil {
		return nil, err
	}

	return &fileWriter{
		WriteCloser: w,
		size:        size,
		tx:          tx,
	}, nil
}

func (d *driver) List(ctx context.Context, path string) ([]string, error) {
	p := d.getCypressPath(path)

	fileNames := make([]string, 0)
	if err := d.client.ListNode(ctx, p, &fileNames, nil); err != nil {
		return nil, d.translatePathResolveError(err, p)
	}

	keys := make([]string, 0, len(fileNames))
	for _, fileName := range fileNames {
		keys = append(keys, d.normalizeOutputPath(p.Child(fileName)))
	}

	return keys, nil
}

func (d *driver) Move(ctx context.Context, sourcePath string, destPath string) error {
	source := d.getCypressPath(sourcePath)
	dest := d.getCypressPath(destPath)

	moveNodeOptions := &yt.MoveNodeOptions{Recursive: true, Force: true}
	_, err := d.client.MoveNode(ctx, source, dest, moveNodeOptions)
	return d.translatePathResolveError(err, source)
}

func (d *driver) Delete(ctx context.Context, path string) error {
	p := d.getCypressPath(path)
	removeNodeOptions := &yt.RemoveNodeOptions{Recursive: true}
	return d.translatePathResolveError(d.client.RemoveNode(ctx, p, removeNodeOptions), p)
}

// Walk traverses a filesystem defined within driver, starting
// from the given path, calling f on each file and directory
func (d *driver) Walk(ctx context.Context, path string, f storagedriver.WalkFn) error {
	return storagedriver.WalkFallback(ctx, d, path, f)
}

// URLFor returns a URL which may be used to retrieve the content stored at the given path.
// May return an UnsupportedMethodErr in certain StorageDriver implementations.
func (d *driver) URLFor(ctx context.Context, path string, options map[string]interface{}) (string, error) {
	return "", storagedriver.ErrUnsupportedMethod{}
}
