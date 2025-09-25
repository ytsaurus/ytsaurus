package client

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"time"
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
