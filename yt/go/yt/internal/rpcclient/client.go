package rpcclient

import (
	"github.com/golang/protobuf/proto"

	"a.yandex-team.ru/yt/go/proto/core/rpc"
)

func setToken(req *rpc.TRequestHeader, token string) error {
	return proto.SetExtension(req, rpc.E_TCredentialsExt_CredentialsExt, &rpc.TCredentialsExt{Token: &token})
}
