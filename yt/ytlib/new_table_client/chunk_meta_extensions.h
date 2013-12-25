#pragma once

#include <core/misc/protobuf_helpers.h>

#include <ytlib/new_table_client/chunk_meta.pb.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

DECLARE_PROTO_EXTENSION(NVersionedTableClient::NProto::TTableSchemaExt, 50)
DECLARE_PROTO_EXTENSION(NVersionedTableClient::NProto::TBlockMetaExt, 51)
DECLARE_PROTO_EXTENSION(NVersionedTableClient::NProto::TBlockIndexExt, 52)
DECLARE_PROTO_EXTENSION(NVersionedTableClient::NProto::TNameTableExt, 53)
DECLARE_PROTO_EXTENSION(NVersionedTableClient::NProto::TKeyColumnsExt, 54)
DECLARE_PROTO_EXTENSION(NVersionedTableClient::NProto::TBoundaryKeysExt, 55)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
