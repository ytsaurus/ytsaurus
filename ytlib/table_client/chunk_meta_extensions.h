#pragma once

#include "public.h"

#include <yt/client/table_client/proto/chunk_meta.pb.h>
#include <yt/client/table_client/unversioned_row.h>

#include <yt/core/misc/protobuf_helpers.h>

#include <yt/core/misc/sync_cache.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

DECLARE_PROTO_EXTENSION(NTableClient::NProto::TTableSchemaExt, 50)
DECLARE_PROTO_EXTENSION(NTableClient::NProto::TBlockMetaExt, 51)
DECLARE_PROTO_EXTENSION(NTableClient::NProto::TNameTableExt, 53)
DECLARE_PROTO_EXTENSION(NTableClient::NProto::TBoundaryKeysExt, 55)
DECLARE_PROTO_EXTENSION(NTableClient::NProto::TSamplesExt, 56)
DECLARE_PROTO_EXTENSION(NTableClient::NProto::TPartitionsExt, 59)
DECLARE_PROTO_EXTENSION(NTableClient::NProto::TColumnMetaExt, 58)
DECLARE_PROTO_EXTENSION(NTableClient::NProto::TColumnarStatisticsExt, 60)

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

///////////////////////////////////////////////////////////////////////////////

class TCachedBlockMeta
    : public TSyncCacheValueBase<NChunkClient::TChunkId, TCachedBlockMeta>
    , public NTableClient::NProto::TBlockMetaExt
{
public:
    TCachedBlockMeta(const NChunkClient::TChunkId& chunkId, NTableClient::NProto::TBlockMetaExt blockMetaExt);
    i64 GetWeight() const;

private:
    i64 Weight_;
};

DEFINE_REFCOUNTED_TYPE(TCachedBlockMeta)

////////////////////////////////////////////////////////////////////////////////

class TBlockMetaCache
    : public TSyncSlruCacheBase<NChunkClient::TChunkId, TCachedBlockMeta>
{
public:
    TBlockMetaCache(TSlruCacheConfigPtr config, const NProfiling::TProfiler& profiler);

private:
    virtual i64 GetWeight(const TCachedBlockMetaPtr& value) const override;
};

DEFINE_REFCOUNTED_TYPE(TBlockMetaCache)

////////////////////////////////////////////////////////////////////////////////

NChunkClient::NProto::TChunkMeta FilterChunkMetaByPartitionTag(
    const NChunkClient::NProto::TChunkMeta& chunkMeta,
    const TCachedBlockMetaPtr& cachedBlockMeta,
    int partitionTag);

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
