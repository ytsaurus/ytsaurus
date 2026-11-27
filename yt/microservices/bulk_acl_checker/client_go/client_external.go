//go:build !internal
// +build !internal

package client

import (
	"context"
	"net/http"

	"go.ytsaurus.tech/library/go/core/log"
)

type Client struct {
	l log.Structured
	c *Config
}

type Config struct {
	BaseURL       string
	GetACLRetries uint8
	// Required by /user-acl-tree.
	Token string
}

func NewClient(c *Config, l log.Structured) (*Client, error) {
	return &Client{
		l: l,
		c: c,
	}, nil
}
func (c *Client) patchRequestWithAuthData(ctx context.Context, request *http.Request) {
	if c.c.Token != "" {
		request.Header.Set("Authorization", "OAuth "+c.c.Token)
	}
}
