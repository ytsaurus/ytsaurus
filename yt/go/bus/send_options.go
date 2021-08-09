package bus

import (
	"time"

	"github.com/golang/protobuf/proto"

	"a.yandex-team.ru/library/go/ptr"
	"a.yandex-team.ru/yt/go/compression"
	"a.yandex-team.ru/yt/go/guid"
	"a.yandex-team.ru/yt/go/proto/core/misc"
	"a.yandex-team.ru/yt/go/proto/core/rpc"
)

type SendOption interface {
	before(req *clientReq)
	after(req *clientReq)
}

// SendOptionBeforeFunc type is an adapter to allow the use of
// ordinary functions as SendOption's before method.
type SendOptionBeforeFunc func(req *clientReq)

func (f SendOptionBeforeFunc) before(req *clientReq) {
	f(req)
}

func (f SendOptionBeforeFunc) after(req *clientReq) {}

// SendOptionAfterFunc type is an adapter to allow the use of
// ordinary functions as SendOption's after method.
type SendOptionAfterFunc func(req *clientReq)

func (f SendOptionAfterFunc) before(req *clientReq) {}

func (f SendOptionAfterFunc) after(req *clientReq) {
	f(req)
}

type sendOption struct {
	Before func(req *clientReq)
	After  func(req *clientReq)
}

func (o *sendOption) before(req *clientReq) {
	o.Before(req)
}

func (o *sendOption) after(req *clientReq) {
	o.After(req)
}

func WithProtocolVersionMajor(v int32) *sendOption {
	return &sendOption{
		Before: func(req *clientReq) {
			req.reqHeader.ProtocolVersionMajor = &v
		},
		After: func(req *clientReq) {},
	}
}

func WithToken(token string) *sendOption {
	return &sendOption{
		Before: func(req *clientReq) {
			_ = proto.SetExtension(
				req.reqHeader,
				rpc.E_TCredentialsExt_CredentialsExt,
				&rpc.TCredentialsExt{Token: &token},
			)
		},
		After: func(req *clientReq) {},
	}
}

func WithUser(user string) *sendOption {
	return &sendOption{
		Before: func(req *clientReq) {
			req.reqHeader.User = ptr.String(user)
		},
		After: func(req *clientReq) {},
	}
}

func WithUserTag(tag string) *sendOption {
	return &sendOption{
		Before: func(req *clientReq) {
			req.reqHeader.UserTag = ptr.String(tag)
		},
		After: func(req *clientReq) {},
	}
}

func WithRequestTimeout(timeout time.Duration) *sendOption {
	return &sendOption{
		Before: func(req *clientReq) {
			req.reqHeader.Timeout = ptr.Int64(durationToMicroseconds(timeout))
		},
		After: func(req *clientReq) {},
	}
}

func WithAckTimeout(timeout time.Duration) *sendOption {
	return &sendOption{
		Before: func(req *clientReq) {
			req.acknowledgementTimeout = ptr.Duration(timeout)
		},
		After: func(req *clientReq) {},
	}
}

func WithoutRequestAcknowledgement() *sendOption {
	return &sendOption{
		Before: func(req *clientReq) {
			req.acknowledgementTimeout = nil
		},
		After: func(req *clientReq) {},
	}
}

func WithRequiredServerFeatureIDs(ids ...int32) *sendOption {
	return &sendOption{
		Before: func(req *clientReq) {
			req.reqHeader.RequiredServerFeatureIds = append(req.reqHeader.RequiredServerFeatureIds, ids...)
		},
		After: func(req *clientReq) {},
	}
}

func WithDeclaredClientFeatureIDs(ids ...int32) *sendOption {
	return &sendOption{
		Before: func(req *clientReq) {
			req.reqHeader.DeclaredClientFeatureIds = append(req.reqHeader.DeclaredClientFeatureIds, ids...)
		},
		After: func(req *clientReq) {},
	}
}

func WithAttachments(attachments ...[]byte) *sendOption {
	return &sendOption{
		Before: func(req *clientReq) {
			req.reqAttachments = append(req.reqAttachments, attachments...)
		},
		After: func(req *clientReq) {},
	}
}

func WithAttachmentStrings(attachments ...string) *sendOption {
	return &sendOption{
		Before: func(req *clientReq) {
			for _, a := range attachments {
				req.reqAttachments = append(req.reqAttachments, []byte(a))
			}
		},
		After: func(req *clientReq) {},
	}
}

func WithResponseAttachments(attachments *[][]byte) *sendOption {
	return &sendOption{
		Before: func(req *clientReq) {},
		After: func(req *clientReq) {
			for i := 0; i < len(req.rspAttachments); i++ {
				*attachments = append(*attachments, req.rspAttachments[i])
			}
		},
	}
}

func WithRequestCodec(codecID compression.CodecID) *sendOption {
	return &sendOption{
		Before: func(req *clientReq) {
			req.reqHeader.RequestCodec = ptr.Int32(int32(codecID))
		},
		After: func(req *clientReq) {},
	}
}

func WithResponseCodec(codecID compression.CodecID) *sendOption {
	return &sendOption{
		Before: func(req *clientReq) {
			req.reqHeader.ResponseCodec = ptr.Int32(int32(codecID))
		},
		After: func(req *clientReq) {},
	}
}

func WithRequestID(id guid.GUID) *sendOption {
	return &sendOption{
		Before: func(req *clientReq) {
			req.id = id
			req.reqHeader.RequestId = misc.NewProtoFromGUID(id)
		},
		After: func(req *clientReq) {},
	}
}
