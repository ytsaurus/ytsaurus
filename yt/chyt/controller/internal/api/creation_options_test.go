package api

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"go.ytsaurus.tech/yt/chyt/controller/internal/auth"
	"go.ytsaurus.tech/yt/chyt/controller/internal/strawberry"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yson"
	"go.ytsaurus.tech/yt/go/yt"
)

type creationOptionsController struct {
	strawberry.Controller
	parseError error
}

func (c creationOptionsController) ParseSpeclet(raw yson.RawValue) (any, error) {
	if c.parseError != nil {
		return nil, c.parseError
	}
	var speclet map[string]any
	if err := yson.Unmarshal(raw, &speclet); err != nil {
		return nil, err
	}
	return speclet, nil
}

func (c creationOptionsController) DescribeOptions(parsed any) []strawberry.OptionGroupDescriptor {
	if len(parsed.(map[string]any)) != 0 {
		panic("creation speclet must be empty")
	}
	return []strawberry.OptionGroupDescriptor{{Title: "Resources", Options: []strawberry.OptionDescriptor{{Name: "instance_cpu", Type: strawberry.TypeInt64, DefaultValue: 8}}}}
}

func TestDescribeCreationOptionsWithoutOplet(t *testing.T) {
	// A nil Cypress client makes any existence, permission, read or write call fail.
	a := &API{ctl: creationOptionsController{}}
	groups, err := a.DescribeCreationOptions()
	require.NoError(t, err)
	require.Equal(t, "General", groups[0].Title)
	require.Equal(t, "Resources", groups[len(groups)-1].Title)
	require.Equal(t, 8, groups[len(groups)-1].Options[0].DefaultValue)
	require.Empty(t, DescribeCreationOptionsCmdDescriptor.Parameters)
	registered := false
	for _, command := range AllCommands {
		if command.Name == "describe_creation_options" {
			registered = true
			require.Empty(t, command.Parameters)
			require.NotNil(t, command.Handler)
		}
	}
	require.True(t, registered, "creation descriptor command must be exposed by the router")
}

func TestDescribeCreationOptionsParseError(t *testing.T) {
	expected := errors.New("invalid controller config")
	a := &API{ctl: creationOptionsController{parseError: expected}}
	_, err := a.DescribeCreationOptions()
	require.ErrorIs(t, err, expected)
}

func TestHandleDescribeCreationOptions(t *testing.T) {
	a := HTTPAPI{API: &API{ctl: creationOptionsController{}}}
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, "/describe_creation_options", strings.NewReader(`{"params":{}}`))
	r.Header.Set("Content-Type", "application/json")
	r.Header.Set("Accept", "application/json")
	params := a.parseAndValidateRequestParams(w, r, DescribeCreationOptionsCmdDescriptor)
	require.NotNil(t, params)
	DescribeCreationOptionsCmdDescriptor.Handler(a, w, r, params)
	require.Equal(t, http.StatusOK, w.Code)
	require.Contains(t, w.Body.String(), `"result"`)
	require.Contains(t, w.Body.String(), `"default_value":8`)
}

func (c creationOptionsController) Family() string { return "chyt" }

type deniedOptionsClient struct {
	yt.Client
	exists      bool
	checkedRead bool
}

func (c *deniedOptionsClient) NodeExists(context.Context, ypath.YPath, *yt.NodeExistsOptions) (bool, error) {
	return c.exists, nil
}

func (c *deniedOptionsClient) CheckPermission(_ context.Context, _ string, permission yt.Permission, _ ypath.YPath, _ *yt.CheckPermissionOptions) (*yt.CheckPermissionResponse, error) {
	c.checkedRead = permission == yt.PermissionRead
	result := &yt.CheckPermissionResponse{}
	result.Action = yt.ActionDeny
	return result, nil
}

func TestDescribeOptionsRetainsExistenceAndReadPermission(t *testing.T) {
	for _, exists := range []bool{false, true} {
		client := &deniedOptionsClient{exists: exists}
		a := &API{Ytc: client, ctl: creationOptionsController{}}
		_, err := a.DescribeOptions(auth.WithRequester(context.Background(), "user"), "clique")
		require.Error(t, err)
		require.Equal(t, exists, client.checkedRead)
		// Embedded nil client panics if a forbidden speclet read is attempted.
	}
}
