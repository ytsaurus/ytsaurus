package ytmock

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yterrors"
)

func TestGetNode(t *testing.T) {
	missingUserError := &yterrors.Error{
		Code:    yterrors.CodeResolveError,
		Message: "No such user: missing-user",
	}
	config := &Config{
		Objects: []Object{
			{
				Path:  "//home/node",
				Type:  yt.NodeMap,
				Value: map[string]string{"key": "value"},
			},
			{
				Path:       "//sys/users/allowed-user",
				Type:       yt.NodeUser,
				Attributes: map[string]any{"banned": false},
			},
			{
				Path:       "//sys/users/banned-user",
				Type:       yt.NodeUser,
				Attributes: map[string]any{"banned": true},
			},
			{
				Path:       ypath.Path("//sys/users").Child(ypath.EscapeLiteral("escaped-user/@©\n")),
				Type:       yt.NodeUser,
				Attributes: map[string]any{"banned": true},
			},
			{
				Path:       "//home/object",
				Type:       yt.NodeMap,
				Attributes: map[string]any{"inherit_acl": false},
			},
			{
				Path:  "//sys/users/missing-user/@banned",
				Error: missingUserError,
			},
		},
	}
	for _, tc := range []struct {
		name          string
		path          ypath.Path
		expected      any
		expectedError error
	}{
		{"value", "//home/node", map[string]any{"key": "value"}, nil},
		{"allowed", "//sys/users/allowed-user/@banned", false, nil},
		{"banned", "//sys/users/banned-user/@banned", true, nil},
		{"escaped", `//sys/users/escaped-user\/\@\xc2\xa9\x0a/@banned`, true, nil},
		{"missing", "//sys/users/missing-user/@banned", nil, missingUserError},
		{"user type", "//sys/users/allowed-user/@type", string(yt.NodeUser), nil},
		{"map type", "//home/object/@type", string(yt.NodeMap), nil},
		{"inherit_acl", "//home/object/@inherit_acl", false, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client := NewClient(t, config)
			var result any
			err := client.GetNode(context.Background(), tc.path, &result, nil)
			if tc.expectedError != nil {
				require.ErrorIs(t, err, tc.expectedError)
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.expected, result)
			}
			require.Len(t, client.GetNodeCalls(), 1)
		})
	}
	t.Run("invalid result", func(t *testing.T) {
		client := NewClient(t, config)
		var result bool
		require.Error(t, client.GetNode(context.Background(), ypath.Path("//home/node"), &result, nil))
	})
}

func TestConfigUpdates(t *testing.T) {
	config := &Config{
		Objects: []Object{{
			Path:       "//home/node",
			Type:       yt.NodeMap,
			Value:      map[string]string{"key": "initial"},
			Attributes: map[string]any{"inherit_acl": true},
		}},
	}
	client := NewClient(t, config)
	for _, tc := range []struct {
		value      string
		inheritACL bool
	}{
		{"initial", true},
		{"updated", false},
	} {
		t.Run(tc.value, func(t *testing.T) {
			config.Objects[0].Value = map[string]string{"key": tc.value}
			config.Objects[0].Attributes["inherit_acl"] = tc.inheritACL
			var value map[string]string
			require.NoError(t, client.GetNode(context.Background(), ypath.Path("//home/node"), &value, nil))
			require.Equal(t, map[string]string{"key": tc.value}, value)
			var inheritACL bool
			require.NoError(t, client.GetNode(context.Background(), ypath.Path("//home/node/@inherit_acl"), &inheritACL, nil))
			require.Equal(t, tc.inheritACL, inheritACL)
		})
	}
}

func TestGetNodeReturnsCopy(t *testing.T) {
	groupsPath := ypath.Path("//sys/users/user/@member_of_closure")
	groups := []string{"users", "admins"}
	client := NewClient(t, &Config{
		Objects: []Object{{
			Path:       "//sys/users/user",
			Type:       yt.NodeUser,
			Attributes: map[string]any{"member_of_closure": groups},
		}},
	})
	for range 2 {
		var result []string
		require.NoError(t, client.GetNode(context.Background(), groupsPath, &result, nil))
		require.Equal(t, []string{"users", "admins"}, result)
		result[0] = "changed"
	}
}

func TestCanceledCalls(t *testing.T) {
	client := NewClient(t, &Config{
		Objects: []Object{{
			Path:       "//sys/users/user",
			Type:       yt.NodeUser,
			Attributes: map[string]any{"banned": false},
		}},
	})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	banned := true
	require.ErrorIs(t, client.GetNode(ctx, ypath.Path("//sys/users/user/@banned"), &banned, nil), context.Canceled)
	require.True(t, banned)
	response, err := client.CheckPermissionByACL(ctx, "user", yt.PermissionRead, nil, nil)
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, response)
	require.Len(t, client.GetNodeCalls(), 1)
	require.Len(t, client.CheckPermissionByACLCalls(), 1)
}

func TestCheckPermissionByACL(t *testing.T) {
	backendError := errors.New("ytserver is unavailable")
	acl := []yt.ACE{
		{Action: yt.ActionAllow, Subjects: []string{"user", "group"}, Permissions: []yt.Permission{yt.PermissionWrite, yt.PermissionRead}},
		{Action: yt.ActionDeny, Subjects: []string{"denied-user"}, Permissions: []yt.Permission{yt.PermissionRead}},
	}
	otherACL := []yt.ACE{
		{Action: yt.ActionAllow, Subjects: []string{"other-user"}, Permissions: []yt.Permission{yt.PermissionRead}},
		{Action: yt.ActionDeny, Subjects: []string{"denied-user"}, Permissions: []yt.Permission{yt.PermissionRead}},
	}
	config := &Config{
		Permissions: []Permission{
			{User: "user", Permission: yt.PermissionRead, ACL: acl, Action: yt.ActionDeny},
			{User: "other-user", Permission: yt.PermissionRead, ACL: acl, Action: yt.ActionAllow},
			{User: "user", Permission: yt.PermissionWrite, ACL: acl, Action: yt.ActionAllow},
			{User: "user", Permission: yt.PermissionRead, ACL: otherACL, Error: backendError},
		},
	}
	client := NewClient(t, config)
	for _, configured := range config.Permissions {
		response, err := client.CheckPermissionByACL(context.Background(), configured.User, configured.Permission, configured.ACL, nil)
		if configured.Error != nil {
			require.ErrorIs(t, err, configured.Error)
			require.Nil(t, response)
		} else {
			require.NoError(t, err)
			require.Equal(t, configured.Action, response.Action)
		}
	}
	reorderedACL := []yt.ACE{
		acl[1],
		{Action: yt.ActionAllow, Subjects: []string{"group", "user"}, Permissions: []yt.Permission{yt.PermissionRead, yt.PermissionWrite}},
	}
	response, err := client.CheckPermissionByACL(context.Background(), "user", yt.PermissionRead, reorderedACL, nil)
	require.NoError(t, err)
	require.Equal(t, yt.ActionDeny, response.Action)
	require.Equal(t, []string{"user", "group"}, acl[0].Subjects)
	require.Equal(t, []yt.Permission{yt.PermissionWrite, yt.PermissionRead}, acl[0].Permissions)
	require.Equal(t, yt.ActionDeny, reorderedACL[0].Action)
	config.Permissions[0].Action = yt.ActionAllow
	response, err = client.CheckPermissionByACL(context.Background(), "user", yt.PermissionRead, acl, nil)
	require.NoError(t, err)
	require.Equal(t, yt.ActionAllow, response.Action)
	require.Equal(t, []CheckPermissionByACLCall{
		{User: "user", Permission: yt.PermissionRead, ACL: acl},
		{User: "other-user", Permission: yt.PermissionRead, ACL: acl},
		{User: "user", Permission: yt.PermissionWrite, ACL: acl},
		{User: "user", Permission: yt.PermissionRead, ACL: otherACL},
		{User: "user", Permission: yt.PermissionRead, ACL: reorderedACL},
		{User: "user", Permission: yt.PermissionRead, ACL: acl},
	}, client.CheckPermissionByACLCalls())
}

func TestCallHistoryDeepCopy(t *testing.T) {
	acl := []yt.ACE{{Action: yt.ActionAllow, Subjects: []string{"user"}, Permissions: []yt.Permission{yt.PermissionRead}}}
	client := NewClient(t, &Config{
		Objects: []Object{{
			Path:       "//sys/users/user",
			Type:       yt.NodeUser,
			Attributes: map[string]any{"banned": false},
		}},
		Permissions: []Permission{{User: "user", Permission: yt.PermissionRead, ACL: acl, Action: yt.ActionAllow}},
	})
	maxSize := int64(1)
	getOptions := &yt.GetNodeOptions{
		MaxSize:           &maxSize,
		MasterReadOptions: &yt.MasterReadOptions{ReadFrom: yt.ReadFromCache},
	}
	var banned bool
	path := ypath.Path("//sys/users/user/@banned")
	require.NoError(t, client.GetNode(context.Background(), path, &banned, getOptions))
	*getOptions.MaxSize = 2
	getOptions.ReadFrom = yt.ReadFromLeader
	getCalls := client.GetNodeCalls()
	require.Equal(t, path, getCalls[0].Path)
	require.Equal(t, int64(1), *getCalls[0].Options.MaxSize)
	require.Equal(t, yt.ReadFromCache, getCalls[0].Options.ReadFrom)
	getCalls[0].Path = ypath.Path("//changed")
	getCalls[0].Options.ReadFrom = yt.ReadFromFollower
	require.Equal(t, path, client.GetNodeCalls()[0].Path)
	require.Equal(t, yt.ReadFromCache, client.GetNodeCalls()[0].Options.ReadFrom)

	permissionOptions := &yt.CheckPermissionByACLOptions{MasterReadOptions: &yt.MasterReadOptions{ReadFrom: yt.ReadFromCache}}
	_, err := client.CheckPermissionByACL(context.Background(), "user", yt.PermissionRead, acl, permissionOptions)
	require.NoError(t, err)
	acl[0].Subjects[0] = "changed"
	permissionOptions.ReadFrom = yt.ReadFromLeader
	permissionCalls := client.CheckPermissionByACLCalls()
	require.Equal(t, "user", permissionCalls[0].User)
	require.Equal(t, yt.PermissionRead, permissionCalls[0].Permission)
	require.Equal(t, []string{"user"}, permissionCalls[0].ACL[0].Subjects)
	require.Equal(t, []yt.Permission{yt.PermissionRead}, permissionCalls[0].ACL[0].Permissions)
	require.Equal(t, yt.ReadFromCache, permissionCalls[0].Options.ReadFrom)
	permissionCalls[0].ACL[0].Subjects[0] = "changed again"
	permissionCalls[0].Options.ReadFrom = yt.ReadFromFollower
	require.Equal(t, []string{"user"}, client.CheckPermissionByACLCalls()[0].ACL[0].Subjects)
	require.Equal(t, yt.ReadFromCache, client.CheckPermissionByACLCalls()[0].Options.ReadFrom)
}

func TestConcurrentCalls(t *testing.T) {
	const requestCount = 32
	config := &Config{}
	for i := range requestCount {
		user := fmt.Sprintf("user-%d", i)
		config.Objects = append(config.Objects, Object{
			Path:       ypath.Path("//sys/users").Child(user),
			Type:       yt.NodeUser,
			Attributes: map[string]any{"banned": false},
		})
		config.Permissions = append(config.Permissions, Permission{User: user, Permission: yt.PermissionRead, Action: yt.ActionAllow})
	}
	client := NewClient(t, config)
	var pending sync.WaitGroup
	for i := range requestCount {
		pending.Add(1)
		go func() {
			defer pending.Done()
			user := fmt.Sprintf("user-%d", i)
			path := ypath.Path("//sys/users").Child(user).Attr("banned")
			var banned bool
			assert.NoError(t, client.GetNode(context.Background(), path, &banned, nil))
			assert.False(t, banned)
			_, err := client.CheckPermissionByACL(context.Background(), user, yt.PermissionRead, nil, nil)
			assert.NoError(t, err)
			client.GetNodeCalls()
			client.CheckPermissionByACLCalls()
		}()
	}
	pending.Wait()
	require.Len(t, client.GetNodeCalls(), requestCount)
	require.Len(t, client.CheckPermissionByACLCalls(), requestCount)
}

type errorRecorder struct {
	messages []string
}

func (*errorRecorder) Helper() {}

func (r *errorRecorder) Errorf(format string, args ...any) {
	r.messages = append(r.messages, fmt.Sprintf(format, args...))
}

func TestUnexpectedCalls(t *testing.T) {
	for _, tc := range []struct {
		name    string
		config  *Config
		call    func(*Client) error
		details []string
	}{
		{
			name:   "get",
			config: &Config{},
			call: func(client *Client) error {
				var value any
				return client.GetNode(context.Background(), ypath.Path("//home/missing"), &value, nil)
			},
			details: []string{"GetNode", "//home/missing"},
		},
		{
			name:   "nil value",
			config: &Config{Objects: []Object{{Path: "//home/node", Value: []string(nil)}}},
			call: func(client *Client) error {
				var value []string
				return client.GetNode(context.Background(), ypath.Path("//home/node"), &value, nil)
			},
			details: []string{"GetNode", "//home/node"},
		},
		{
			name: "attributes",
			config: &Config{Objects: []Object{{
				Path:       "//home/node",
				Type:       yt.NodeMap,
				Value:      map[string]string{"key": "value"},
				Attributes: map[string]any{"opaque": false},
			}}},
			call: func(client *Client) error {
				var value any
				return client.GetNode(context.Background(), ypath.Path("//home/node"), &value, &yt.GetNodeOptions{Attributes: []string{"opaque"}})
			},
			details: []string{"GetNode", "//home/node", "opaque"},
		},
		{
			name:   "permission",
			config: &Config{},
			call: func(client *Client) error {
				_, err := client.CheckPermissionByACL(context.Background(), "some-user", yt.PermissionWrite, nil, nil)
				return err
			},
			details: []string{"CheckPermissionByACL", "some-user", "write"},
		},
		{
			name:   "permission without action",
			config: &Config{Permissions: []Permission{{User: "some-user", Permission: yt.PermissionRead}}},
			call: func(client *Client) error {
				_, err := client.CheckPermissionByACL(context.Background(), "some-user", yt.PermissionRead, nil, nil)
				return err
			},
			details: []string{"CheckPermissionByACL", "some-user", "read"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reporter := &errorRecorder{}
			client := NewClient(reporter, tc.config)
			completed := make(chan error, 1)
			go func() { completed <- tc.call(client) }()
			select {
			case err := <-completed:
				require.Error(t, err)
				require.Equal(t, []string{err.Error()}, reporter.messages)
				for _, detail := range tc.details {
					require.Contains(t, err.Error(), detail)
				}
			case <-time.After(time.Second):
				t.Fatal("unexpected call did not return an error")
			}
		})
	}
}
