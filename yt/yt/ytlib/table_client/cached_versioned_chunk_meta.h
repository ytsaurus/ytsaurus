#pragma once

#include "public.h"
#include "chunk_index.h"
#include "chunk_meta_extensions.h"
#include "columnar_chunk_meta.h"

#include <yt/yt/ytlib/chunk_client/chunk_meta_extensions.h>

#include <yt/yt/ytlib/chunk_client/public.h>

#include <yt/yt/ytlib/node_tracker_client/public.h>

#include <yt/yt/ytlib/columnar_chunk_format/prepared_meta.h>

#include <yt/yt/core/misc/memory_usage_tracker.h>

#include <yt/yt/core/actions/future.h>

#include <library/cpp/yt/memory/atomic_intrusive_ptr.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

struct THashTableChunkIndexMeta
{
    struct TBlockMeta
    {
        TBlockMeta(
            int blockIndex,
            const TIndexedVersionedBlockFormatDetail& indexedBlockFormatDetail,
            const NProto::THashTableChunkIndexSystemBlockMeta& hashTableChunkIndexSystemBlockMetaExt);

        const int BlockIndex;
        const THashTableChunkIndexFormatDetail FormatDetail;
        const TLegacyOwningKey BlockLastKey;
    };

    TIndexedVersionedBlockFormatDetail IndexedBlockFormatDetail;
    std::vector<TBlockMeta> BlockMetas;

    explicit THashTableChunkIndexMeta(const TTableSchemaPtr& schema);

    i64 GetMemoryUsage() const;
};

struct TXorFilterMeta
{
    struct TBlockMeta
    {
        TBlockMeta(
            int blockIndex,
            const NProto::TXorFilterSystemBlockMeta& xorFilterSystemBlockMetaExt);

        const int BlockIndex;
        const TLegacyOwningKey BlockLastKey;
    };

    int KeyPrefixLength;
    std::vector<TBlockMeta> BlockMetas;

    i64 GetMemoryUsage() const;
};

////////////////////////////////////////////////////////////////////////////////

class TCachedVersionedChunkMeta
    : public TColumnarChunkMeta
{
public:
    DEFINE_BYREF_RO_PROPERTY(std::optional<THashTableChunkIndexMeta>, HashTableChunkIndexMeta);

    static TCachedVersionedChunkMetaPtr Create(
        bool preparedColumnarMeta,
        const IMemoryUsageTrackerPtr& memoryTracker,
        const NChunkClient::TRefCountedChunkMetaPtr& chunkMeta);

    bool IsColumnarMetaPrepared() const;

    i64 GetMemoryUsage() const override;

    TIntrusivePtr<NColumnarChunkFormat::TPreparedChunkMeta> GetPreparedChunkMeta(NColumnarChunkFormat::IBlockDataProvider* blockProvider = nullptr);

    int GetChunkKeyColumnCount() const;

    const TXorFilterMeta* FindXorFilterByLength(int keyPrefixLength) const;

private:
    TCachedVersionedChunkMeta(
        bool prepareColumnarMeta,
        const IMemoryUsageTrackerPtr& memoryTracker,
        const NChunkClient::NProto::TChunkMeta& chunkMeta);

    const bool ColumnarMetaPrepared_;

    TMemoryUsageTrackerGuard MemoryTrackerGuard_;

    TAtomicIntrusivePtr<NColumnarChunkFormat::TPreparedChunkMeta> PreparedMeta_;
    std::atomic<size_t> PreparedMetaSize_ = 0;

    std::map<int, TXorFilterMeta> XorFilterMetaByLength_;

    DECLARE_NEW_FRIEND()


    void ParseHashTableChunkIndexMeta(const NProto::TSystemBlockMetaExt& systemBlockMetaExt);
    void ParseXorFilterMeta(const NProto::TSystemBlockMetaExt& systemBlockMetaExt);
};

DEFINE_REFCOUNTED_TYPE(TCachedVersionedChunkMeta)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
