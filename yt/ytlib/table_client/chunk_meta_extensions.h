#pragma once

#include "public.h"

#include <core/misc/protobuf_helpers.h>

#include <ytlib/table_client/table_chunk_meta.pb.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

DECLARE_PROTO_EXTENSION(NTableClient::NProto::TChannelsExt, 10)
DECLARE_PROTO_EXTENSION(NTableClient::NProto::TOldSamplesExt, 11)
DECLARE_PROTO_EXTENSION(NTableClient::NProto::TIndexExt, 12)
DECLARE_PROTO_EXTENSION(NTableClient::NProto::TOldBoundaryKeysExt, 13)
DECLARE_PROTO_EXTENSION(NTableClient::NProto::TKeyColumnsExt, 14)
DECLARE_PROTO_EXTENSION(NTableClient::NProto::TPartitionsExt, 15)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
