#pragma once

#include "public.h"

#include "unversioned_row.h"

#include <ytlib/new_table_client/chunk_meta.pb.h>

#include <ytlib/table_client/table_chunk_meta.pb.h>

#include <core/misc/protobuf_helpers.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

DECLARE_PROTO_EXTENSION(NVersionedTableClient::NProto::TTableSchemaExt, 50)
DECLARE_PROTO_EXTENSION(NVersionedTableClient::NProto::TBlockMetaExt, 51)
DECLARE_PROTO_EXTENSION(NVersionedTableClient::NProto::TBlockIndexExt, 52)
DECLARE_PROTO_EXTENSION(NVersionedTableClient::NProto::TNameTableExt, 53)
DECLARE_PROTO_EXTENSION(NVersionedTableClient::NProto::TBoundaryKeysExt, 55)
DECLARE_PROTO_EXTENSION(NVersionedTableClient::NProto::TSamplesExt, 56)

// Moved from old table client
DECLARE_PROTO_EXTENSION(NVersionedTableClient::NProto::TKeyColumnsExt, 14)
DECLARE_PROTO_EXTENSION(NVersionedTableClient::NProto::TPartitionsExt, 15)

////////////////////////////////////////////////////////////////////////////////

namespace NVersionedTableClient {

void GetBoundaryKeys(
    const NChunkClient::NProto::TChunkMeta& chunkMeta, 
    TOwningKey* minKey, 
    TOwningKey* maxKey);

NChunkClient::NProto::TChunkMeta FilterChunkMetaByPartitionTag(
    const NChunkClient::NProto::TChunkMeta& chunkMeta,
    int partitionTag);

NProto::TBoundaryKeysExt EmptyBoundaryKeys();

} // namespace NVersionedTableClient

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
