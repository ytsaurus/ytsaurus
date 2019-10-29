package rpcclient

import (
	"a.yandex-team.ru/yt/go/proto/core/rpc"
	"github.com/golang/protobuf/proto"
)

func setToken(req *rpc.TRequestHeader, token string) error {
	return proto.SetExtension(req, rpc.E_TCredentialsExt_CredentialsExt, &rpc.TCredentialsExt{Token: &token})
}
