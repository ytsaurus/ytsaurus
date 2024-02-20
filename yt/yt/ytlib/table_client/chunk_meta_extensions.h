#pragma once

#include "public.h"

#include <yt/yt_proto/yt/client/table_chunk_format/proto/chunk_meta.pb.h>

#include <yt/yt/client/table_client/unversioned_row.h>

#include <yt/yt/core/misc/protobuf_helpers.h>

#include <yt/yt/core/misc/sync_cache.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

DECLARE_PROTO_EXTENSION(NTableClient::NProto::TTableSchemaExt, 50)
DECLARE_PROTO_EXTENSION(NTableClient::NProto::TDataBlockMetaExt, 51)
DECLARE_PROTO_EXTENSION(NTableClient::NProto::TNameTableExt, 53)
DECLARE_PROTO_EXTENSION(NTableClient::NProto::TBoundaryKeysExt, 55)
DECLARE_PROTO_EXTENSION(NTableClient::NProto::TSamplesExt, 56)
DECLARE_PROTO_EXTENSION(NTableClient::NProto::TPartitionsExt, 59)
DECLARE_PROTO_EXTENSION(NTableClient::NProto::TColumnMetaExt, 58)
DECLARE_PROTO_EXTENSION(NTableClient::NProto::TColumnarStatisticsExt, 60)
DECLARE_PROTO_EXTENSION(NTableClient::NProto::THeavyColumnStatisticsExt, 61)
DECLARE_PROTO_EXTENSION(NTableClient::NProto::TKeyColumnsExt, 14)
DECLARE_PROTO_EXTENSION(NTableClient::NProto::THunkChunkRefsExt, 62)
DECLARE_PROTO_EXTENSION(NTableClient::NProto::THunkChunkMiscExt, 63)
DECLARE_PROTO_EXTENSION(NTableClient::NProto::THunkChunkMetasExt, 64)
DECLARE_PROTO_EXTENSION(NTableClient::NProto::TSystemBlockMetaExt, 65)
DECLARE_PROTO_EXTENSION(NTableClient::NProto::TVersionedRowDigestExt, 66)
DECLARE_PROTO_EXTENSION(NTableClient::NProto::TColumnGroupInfosExt, 67)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

struct TOwningBoundaryKeys
{
    TLegacyOwningKey MinKey;
    TLegacyOwningKey MaxKey;

    void Persist(const TStreamPersistenceContext& context);

    size_t SpaceUsed() const;

    bool operator ==(const TOwningBoundaryKeys& other) const;
    bool operator !=(const TOwningBoundaryKeys& other) const;
};

TString ToString(const TOwningBoundaryKeys& keys);

void Serialize(const TOwningBoundaryKeys& keys, NYson::IYsonConsumer* consumer);
void Deserialize(TOwningBoundaryKeys& key, const NYTree::INodePtr& node);

////////////////////////////////////////////////////////////////////////////////

//! Find owning boundary keys in chunk meta.
//! If keyColumnCount is present and boundary keys are shorter than it,
//! keys are padded with nulls (this is the case for alter-added key columns,
//! see test_tables.py::test_alter_key_column).
bool FindBoundaryKeys(
    const NChunkClient::NProto::TChunkMeta& chunkMeta,
    TLegacyOwningKey* minKey,
    TLegacyOwningKey* maxKey,
    std::optional<int> keyColumnCount = std::nullopt);

std::unique_ptr<TOwningBoundaryKeys> FindBoundaryKeys(
    const NChunkClient::NProto::TChunkMeta& chunkMeta,
    std::optional<int> keyColumnCount = std::nullopt);

///////////////////////////////////////////////////////////////////////////////

bool FindBoundaryKeyBounds(
    const NChunkClient::NProto::TChunkMeta& chunkMeta,
    TOwningKeyBound* lowerBound,
    TOwningKeyBound* upperBound);

///////////////////////////////////////////////////////////////////////////////

class TCachedBlockMeta
    : public TSyncCacheValueBase<NChunkClient::TChunkId, TCachedBlockMeta>
    , public NTableClient::NProto::TDataBlockMetaExt
{
public:
    TCachedBlockMeta(
        NChunkClient::TChunkId chunkId,
        NTableClient::NProto::TDataBlockMetaExt blockMetaExt);
    i64 GetWeight() const;

private:
    const i64 Weight_;
};

DEFINE_REFCOUNTED_TYPE(TCachedBlockMeta)

////////////////////////////////////////////////////////////////////////////////

class TBlockMetaCache
    : public TMemoryTrackingSyncSlruCacheBase<NChunkClient::TChunkId, TCachedBlockMeta>
{
public:
    TBlockMetaCache(
        TSlruCacheConfigPtr config,
        IMemoryUsageTrackerPtr memoryTracker,
        const NProfiling::TProfiler& profiler);

private:
    i64 GetWeight(const TCachedBlockMetaPtr& value) const override;
};

DEFINE_REFCOUNTED_TYPE(TBlockMetaCache)

////////////////////////////////////////////////////////////////////////////////

NChunkClient::NProto::TChunkMeta FilterChunkMetaByPartitionTag(
    const NChunkClient::NProto::TChunkMeta& chunkMeta,
    const TCachedBlockMetaPtr& cachedBlockMeta,
    int partitionTag);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
