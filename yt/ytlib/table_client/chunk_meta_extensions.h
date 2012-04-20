#pragma once

#include <ytlib/table_client/table_chunk_meta.pb.h>
#include <ytlib/misc/protobuf_helpers.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

DECLARE_PROTO_EXTENSION(NTableClient::NProto::TChannels, 10)
DECLARE_PROTO_EXTENSION(NTableClient::NProto::TSamples, 11)
DECLARE_PROTO_EXTENSION(NTableClient::NProto::TIndex, 12)
DECLARE_PROTO_EXTENSION(NTableClient::NProto::TBoundaryKeys, 13)
DECLARE_PROTO_EXTENSION(NTableClient::NProto::TKeyColumns, 14)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
