#pragma once

#include <yt/yt/core/misc/protobuf_helpers.h>

#include <yt/yt/ytlib/chunk_client/proto/data_source.pb.h>
#include <yt/yt/ytlib/chunk_client/proto/data_sink.pb.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

DECLARE_PROTO_EXTENSION(NChunkClient::NProto::TDataSourceDirectoryExt, 420)
DECLARE_PROTO_EXTENSION(NChunkClient::NProto::TDataSinkDirectoryExt, 421)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
