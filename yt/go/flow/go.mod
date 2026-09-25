// The Flow Go SDK is a module of its own so that it can be released at the Flow
// version: the tag flow/X.Y.Z is mirrored as yt/go/flow/vX.Y.Z.
//
// This file also carves the flow subtree out of go.ytsaurus.tech/yt/go. The required
// yt/go version must therefore be one that is released after the carve-out: an earlier
// release still ships the flow packages and makes every import of them ambiguous.
// Inside this repository the module builds against the in-tree yt/go instead, through
// the replace directive below; a consumer ignores the directive and gets the required
// release.
module go.ytsaurus.tech/yt/go/flow

go 1.24.0

require (
	github.com/google/pprof v0.0.0-20250607225305-033d6d78b36a
	github.com/stretchr/testify v1.10.0
	go.ytsaurus.tech/library/go/core/log v0.0.5
	go.ytsaurus.tech/library/go/core/xerrors v0.0.4
	go.ytsaurus.tech/yt/go v0.0.35
	google.golang.org/grpc v1.71.0
	google.golang.org/protobuf v1.36.8
)

require (
	github.com/davecgh/go-spew v1.1.2-0.20180830191138-d8f796af33cc // indirect
	github.com/gofrs/uuid v4.4.0+incompatible // indirect
	github.com/pmezard/go-difflib v1.0.1-0.20181226105442-5d4384ee4fb2 // indirect
	go.uber.org/multierr v1.11.0 // indirect
	go.uber.org/zap v1.27.0 // indirect
	go.ytsaurus.tech/library/go/ptr v0.0.2 // indirect
	go.ytsaurus.tech/library/go/x/xreflect v0.0.3 // indirect
	go.ytsaurus.tech/library/go/x/xruntime v0.0.4 // indirect
	golang.org/x/net v0.42.0 // indirect
	golang.org/x/sys v0.35.0 // indirect
	golang.org/x/text v0.28.0 // indirect
	golang.org/x/xerrors v0.0.0-20240903120638-7835f813f4da // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20250528174236-200df99c418a // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

replace go.ytsaurus.tech/yt/go => ../
