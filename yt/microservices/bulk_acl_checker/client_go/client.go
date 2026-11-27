package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	"go.ytsaurus.tech/yt/go/yterrors"
	bac_lib "go.ytsaurus.tech/yt/microservices/bulk_acl_checker/lib_go"
)

type ACLCheckRequest struct {
	Cluster    string   `json:"cluster"`
	Subject    string   `json:"subject"`
	Permission string   `json:"permission"`
	Paths      []string `json:"paths"`
}

type ACLCheckResponse struct {
	Actions []string `json:"actions"`
}

type UserACLTreeRequest struct {
	Cluster  string
	Path     string
	Username string
	// Permission is read when empty.
	Permission string
}

func (c *Client) performACLCheck(ctx context.Context, r *ACLCheckRequest) (response ACLCheckResponse, err error) {
	jsonRequest, err := json.Marshal(r)
	if err != nil {
		return
	}

	request, err := http.NewRequest("POST", c.c.BaseURL+"/check-acl", bytes.NewBuffer(jsonRequest))
	if err != nil {
		return
	}
	request = request.WithContext(ctx)

	c.patchRequestWithAuthData(ctx, request)

	httpResponse, err := http.DefaultClient.Do(request)
	if err != nil {
		return
	}
	defer httpResponse.Body.Close()

	body, err := io.ReadAll(httpResponse.Body)
	if err != nil {
		return
	}

	var data any
	err = json.Unmarshal(body, &data)
	if err != nil {
		return
	}

	response = ACLCheckResponse{}
	err = json.Unmarshal(body, &response)
	return
}

func (c *Client) CheckACL(ctx context.Context, r *ACLCheckRequest) (response ACLCheckResponse, err error) {
	if r == nil {
		return
	}

	response, err = c.performACLCheck(ctx, r)
	if err != nil {
		return
	}

	retriesLeft := c.c.GetACLRetries
	sleepTime := 50 * time.Millisecond
	for retriesLeft > 0 {
		var errorPaths []string
		var errorIndices []int

		for i, action := range response.Actions {
			if i < len(r.Paths) && action == "error" {
				errorPaths = append(errorPaths, r.Paths[i])
				errorIndices = append(errorIndices, i)
			}
		}

		if len(errorPaths) == 0 {
			break
		}

		retryRequest := &ACLCheckRequest{
			Cluster:    r.Cluster,
			Subject:    r.Subject,
			Permission: r.Permission,
			Paths:      errorPaths,
		}

		retryResponse, err := c.performACLCheck(ctx, retryRequest)
		if err != nil {
			break
		}

		for i, action := range retryResponse.Actions {
			if i < len(errorIndices) {
				originalIndex := errorIndices[i]
				if originalIndex < len(response.Actions) {
					response.Actions[originalIndex] = action
				}
			}
		}
		time.Sleep(sleepTime)
		sleepTime = 2 * sleepTime
		if sleepTime > 5*time.Second {
			sleepTime = 5 * time.Second
		}
		retriesLeft--
	}
	return
}

// GetUserACLTree returns the error of Bulk ACL Checker as a YT error.
func (c *Client) GetUserACLTree(ctx context.Context, r *UserACLTreeRequest) (*bac_lib.UserACLTreeResponse, error) {
	query := url.Values{"path": {r.Path}, "username": {r.Username}}
	if r.Permission != "" {
		query.Set("permission", r.Permission)
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, c.c.BaseURL+"/user-acl-tree/"+url.PathEscape(r.Cluster)+"?"+query.Encode(), nil)
	if err != nil {
		return nil, err
	}
	c.patchRequestWithAuthData(ctx, request)

	httpResponse, err := http.DefaultClient.Do(request)
	if err != nil {
		return nil, err
	}
	defer httpResponse.Body.Close()

	body, err := io.ReadAll(httpResponse.Body)
	if err != nil {
		return nil, err
	}
	if httpResponse.StatusCode != http.StatusOK {
		var ytError yterrors.Error
		if err := json.Unmarshal(body, &ytError); err != nil || ytError.Code == 0 {
			return nil, fmt.Errorf("unexpected response of Bulk ACL Checker with HTTP status %d: %s", httpResponse.StatusCode, body)
		}
		return nil, &ytError
	}
	var response bac_lib.UserACLTreeResponse
	if err := json.Unmarshal(body, &response); err != nil {
		return nil, err
	}
	return &response, nil
}
