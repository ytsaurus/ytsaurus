#pragma once

#include "public.h"
#include "unversioned_row.h"

#include <yt/ytlib/table_client/chunk_meta.pb.h>
//#include <yt/ytlib/table_client/legacy_chunk_meta.pb.h>

#include <yt/core/misc/protobuf_helpers.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

DECLARE_PROTO_EXTENSION(NTableClient::NProto::TTableSchemaExt, 50)
DECLARE_PROTO_EXTENSION(NTableClient::NProto::TBlockMetaExt, 51)
DECLARE_PROTO_EXTENSION(NTableClient::NProto::TNameTableExt, 53)
DECLARE_PROTO_EXTENSION(NTableClient::NProto::TBoundaryKeysExt, 55)
DECLARE_PROTO_EXTENSION(NTableClient::NProto::TSamplesExt, 56)
DECLARE_PROTO_EXTENSION(NTableClient::NProto::TPartitionsExt, 59)
DECLARE_PROTO_EXTENSION(NTableClient::NProto::TColumnMetaExt, 58)

// Moved from old table client.
DECLARE_PROTO_EXTENSION(NTableClient::NProto::TKeyColumnsExt, 14)

////////////////////////////////////////////////////////////////////////////////

namespace NTableClient {

struct TOwningBoundaryKeys
{
    TOwningKey MinKey;
    TOwningKey MaxKey;

    void Persist(const TStreamPersistenceContext& context);

    size_t SpaceUsed() const;

    bool operator ==(const TOwningBoundaryKeys& other) const;
    bool operator !=(const TOwningBoundaryKeys& other) const;
};

TString ToString(const TOwningBoundaryKeys& keys);

void Serialize(const TOwningBoundaryKeys& keys, NYson::IYsonConsumer* consumer);

////////////////////////////////////////////////////////////////////////////////

bool FindBoundaryKeys(
    const NChunkClient::NProto::TChunkMeta& chunkMeta,
    TOwningKey* minKey,
    TOwningKey* maxKey);

std::unique_ptr<TOwningBoundaryKeys> FindBoundaryKeys(
    const NChunkClient::NProto::TChunkMeta& chunkMeta);

NChunkClient::NProto::TChunkMeta FilterChunkMetaByPartitionTag(
    const NChunkClient::NProto::TChunkMeta& chunkMeta,
    int partitionTag);

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
