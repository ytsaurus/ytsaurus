#pragma once

#include <ytlib/table_client/table_chunk_meta.pb.h>
#include <ytlib/misc/protobuf_helpers.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

DECLARE_PROTO_EXTENSION(NTableClient::NProto::TChannelsExt, 10)
DECLARE_PROTO_EXTENSION(NTableClient::NProto::TSamplesExt, 11)
DECLARE_PROTO_EXTENSION(NTableClient::NProto::TIndexExt, 12)
DECLARE_PROTO_EXTENSION(NTableClient::NProto::TBoundaryKeysExt, 13)
DECLARE_PROTO_EXTENSION(NTableClient::NProto::TKeyColumnsExt, 14)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
