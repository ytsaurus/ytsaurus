package ytmock

import (
	"cmp"
	"context"
	"fmt"
	"reflect"
	"slices"
	"sync"

	"github.com/mitchellh/copystructure"

	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yson"
	"go.ytsaurus.tech/yt/go/yt"
)

type TestingT interface {
	Helper()
	Errorf(string, ...any)
}

// Config may be changed between calls, but must not be modified while a call is in progress.
type Config struct {
	Objects     []Object
	Permissions []Permission
}

type Object struct {
	Path ypath.Path
	Type yt.NodeType
	// GetNode on Path is answered only when Value or Error is set.
	Value      any
	Attributes map[string]any
	// Error is returned for an exact Path match.
	Error error
}

type Permission struct {
	User       string
	Permission yt.Permission
	ACL        []yt.ACE
	// CheckPermissionByACL with matching arguments is answered only when Action or Error is set.
	Action yt.SecurityAction
	Error  error
}

type GetNodeCall struct {
	Path    ypath.YPath
	Options *yt.GetNodeOptions
}

type CheckPermissionByACLCall struct {
	User       string
	Permission yt.Permission
	ACL        []yt.ACE
	Options    *yt.CheckPermissionByACLOptions
}

// Client mocks GetNode and CheckPermissionByACL. Other yt.Client methods panic.
// GetNode with GetNodeOptions.Attributes is unexpected.
type Client struct {
	yt.Client
	t      TestingT
	config *Config

	mu                        sync.Mutex
	getNodeCalls              []GetNodeCall
	checkPermissionByACLCalls []CheckPermissionByACLCall
}

func NewClient(t TestingT, config *Config) *Client {
	t.Helper()
	return &Client{
		t:      t,
		config: config,
	}
}

func (c *Client) GetNode(ctx context.Context, path ypath.YPath, result any, options *yt.GetNodeOptions) error {
	c.t.Helper()
	call := GetNodeCall{Path: path, Options: options}
	c.mu.Lock()
	c.getNodeCalls = append(c.getNodeCalls, *clone(&call))
	c.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	nodePath := path.YPath()
	if options != nil && len(options.Attributes) != 0 {
		return c.unexpected("GetNode(path=%q, attributes=%q)", nodePath, options.Attributes)
	}
	value, err := c.getNodeValue(nodePath)
	if err != nil {
		return err
	}
	data, err := yson.Marshal(value)
	if err != nil {
		c.t.Errorf("ytmock: encode GetNode response for %q: %v", nodePath, err)
		return err
	}
	return yson.Unmarshal(data, result)
}

func (c *Client) getNodeValue(path ypath.Path) (any, error) {
	c.t.Helper()
	// Explicit paths take precedence over derived attributes.
	for _, object := range c.config.Objects {
		if path == object.Path && (!isNil(object.Value) || object.Error != nil) {
			return object.Value, object.Error
		}
	}
	for _, object := range c.config.Objects {
		if object.Type != "" && path == object.Path.Attr("type") {
			return object.Type, nil
		}
		for attribute, value := range object.Attributes {
			if path == object.Path.Attr(ypath.EscapeLiteral(attribute)) {
				return value, nil
			}
		}
	}
	return nil, c.unexpected("GetNode(path=%q)", path)
}

func isNil(value any) bool {
	if value == nil {
		return true
	}
	switch v := reflect.ValueOf(value); v.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return v.IsNil()
	}
	return false
}

func (c *Client) CheckPermissionByACL(ctx context.Context, user string, permission yt.Permission, acl []yt.ACE, options *yt.CheckPermissionByACLOptions) (*yt.CheckPermissionResponse, error) {
	c.t.Helper()
	call := CheckPermissionByACLCall{User: user, Permission: permission, ACL: acl, Options: options}
	c.mu.Lock()
	c.checkPermissionByACLCalls = append(c.checkPermissionByACLCalls, *clone(&call))
	c.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	for _, configured := range c.config.Permissions {
		if configured.User != user || configured.Permission != permission || !equalACL(configured.ACL, acl) {
			continue
		}
		if configured.Action == "" && configured.Error == nil {
			continue
		}
		if configured.Error != nil {
			return nil, configured.Error
		}
		return &yt.CheckPermissionResponse{
			CheckPermissionResult: yt.CheckPermissionResult{Action: configured.Action},
		}, nil
	}
	return nil, c.unexpected("CheckPermissionByACL(user=%q, permission=%q, acl=%+v)", user, permission, acl)
}

func equalACL(a, b []yt.ACE) bool {
	if len(a) != len(b) {
		return false
	}
	a, b = normalizeACL(a), normalizeACL(b)
	for _, ace := range a {
		index := slices.IndexFunc(b, func(other yt.ACE) bool {
			return reflect.DeepEqual(ace, other)
		})
		if index == -1 {
			return false
		}
		b = slices.Delete(b, index, index+1)
	}
	return true
}

// normalizeACL returns a copy of acl with sorted slices; empty fields, which the Go client omits,
// take the values YT assumes for them.
func normalizeACL(acl []yt.ACE) []yt.ACE {
	acl = *clone(&acl)
	for i := range acl {
		acl[i].Subjects = sortedOrNil(acl[i].Subjects)
		acl[i].Permissions = sortedOrNil(acl[i].Permissions)
		acl[i].Columns = sortedOrNil(acl[i].Columns)
		if acl[i].InheritanceMode == yt.InheritanceModeNone {
			acl[i].InheritanceMode = yt.InheritanceModeObjectAndDescendants
		}
	}
	return acl
}

func sortedOrNil[T cmp.Ordered](values []T) []T {
	if len(values) == 0 {
		return nil
	}
	slices.Sort(values)
	return values
}

func (c *Client) GetNodeCalls() []GetNodeCall {
	c.mu.Lock()
	defer c.mu.Unlock()
	return *clone(&c.getNodeCalls)
}

func (c *Client) CheckPermissionByACLCalls() []CheckPermissionByACLCall {
	c.mu.Lock()
	defer c.mu.Unlock()
	return *clone(&c.checkPermissionByACLCalls)
}

func (c *Client) unexpected(format string, args ...any) error {
	c.t.Helper()
	err := fmt.Errorf("ytmock: unexpected "+format, args...)
	c.t.Errorf("%v", err)
	return err
}

func clone[T any](value *T) *T {
	return copystructure.Must(copystructure.Copy(value)).(*T)
}
