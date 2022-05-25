package app

import (
	"context"

	"google.golang.org/grpc"

	"a.yandex-team.ru/yt/go/ytprof/api"
)

func (a *App) List(ctx context.Context, in *api.ListRequest, opts ...grpc.CallOption) (*api.ListResponse, error) {
	return &api.ListResponse{Metadata: "hello world!"}, nil
}
