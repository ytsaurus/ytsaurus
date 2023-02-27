#pragma once

#include "public.h"
#include "chunk_index.h"
#include "chunk_meta_extensions.h"
#include "columnar_chunk_meta.h"

#include <yt/yt/ytlib/chunk_client/chunk_meta_extensions.h>

#include <yt/yt/ytlib/chunk_client/public.h>

#include <yt/yt/ytlib/node_tracker_client/public.h>

#include <yt/yt/ytlib/new_table_client/prepared_meta.h>

#include <yt/yt/core/misc/memory_usage_tracker.h>

#include <yt/yt/core/actions/future.h>

#include <library/cpp/yt/memory/atomic_intrusive_ptr.h>

#include <memory>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

struct THashTableChunkIndexMeta
{
    struct TChunkIndexBlockMeta
    {
        TChunkIndexBlockMeta(
            int blockIndex,
            const TIndexedVersionedBlockFormatDetail& indexedBlockFormatDetail,
            const NProto::THashTableChunkIndexSystemBlockMeta& hashTableChunkIndexSystemBlockMetaExt);

        int BlockIndex;
        THashTableChunkIndexFormatDetail FormatDetail;
        TLegacyOwningKey BlockLastKey;
    };

    explicit THashTableChunkIndexMeta(const TTableSchemaPtr& schema);

    TIndexedVersionedBlockFormatDetail IndexedBlockFormatDetail;
    std::vector<TChunkIndexBlockMeta> ChunkIndexBlockMetas;
};

////////////////////////////////////////////////////////////////////////////////

class TCachedVersionedChunkMeta
    : public TColumnarChunkMeta
{
public:
    DEFINE_BYREF_RO_PROPERTY(NTableClient::NProto::THunkChunkRefsExt, HunkChunkRefsExt);
    DEFINE_BYREF_RO_PROPERTY(NTableClient::NProto::THunkChunkMetasExt, HunkChunkMetasExt);

    DEFINE_BYREF_RO_PROPERTY(std::optional<THashTableChunkIndexMeta>, HashTableChunkIndexMeta);

    static TCachedVersionedChunkMetaPtr Create(
        bool preparedColumnarMeta,
        const IMemoryUsageTrackerPtr& memoryTracker,
        const NChunkClient::TRefCountedChunkMetaPtr& chunkMeta);

    bool IsColumnarMetaPrepared() const;

    i64 GetMemoryUsage() const override;

    TIntrusivePtr<NNewTableClient::TPreparedChunkMeta> GetPreparedChunkMeta(NNewTableClient::IBlockDataProvider* blockProvider = nullptr);

    int GetChunkKeyColumnCount() const;

private:
    TCachedVersionedChunkMeta(
        bool prepareColumnarMeta,
        const IMemoryUsageTrackerPtr& memoryTracker,
        const NChunkClient::NProto::TChunkMeta& chunkMeta);

    const bool ColumnarMetaPrepared_;

    TMemoryUsageTrackerGuard MemoryTrackerGuard_;

    TAtomicIntrusivePtr<NNewTableClient::TPreparedChunkMeta> PreparedMeta_;
    size_t PreparedMetaSize_ = 0;

    DECLARE_NEW_FRIEND();


    void ParseHashTableChunkIndexMeta(const NProto::TSystemBlockMetaExt& systemBlockMetaExt);
};

DEFINE_REFCOUNTED_TYPE(TCachedVersionedChunkMeta)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
