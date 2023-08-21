#pragma once

#include "chunk_meta_extensions.h"
#include "timing_reader.h"

#include <yt/yt/client/chunk_client/reader_base.h>
#include <yt/yt/client/chunk_client/read_limit.h>

#include <yt/yt/client/table_client/comparator.h>

#include <yt/yt/ytlib/chunk_client/chunk_meta_extensions.h>
#include <yt/yt/ytlib/chunk_client/block_fetcher.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

// Non-columnar chunk reader base.
class TChunkReaderBase
    : public virtual NChunkClient::IReaderBase
    , public TTimingReaderBase
{
public:
    TChunkReaderBase(
        NChunkClient::TBlockFetcherConfigPtr config,
        NChunkClient::IChunkReaderPtr underlyingReader,
        NChunkClient::IBlockCachePtr blockCache,
        const NChunkClient::TClientChunkReadOptions& chunkReadOptions,
        const NChunkClient::TChunkReaderMemoryManagerPtr& memoryManager = nullptr);

    ~TChunkReaderBase();

    NChunkClient::NProto::TDataStatistics GetDataStatistics() const override;

    NChunkClient::TCodecStatistics GetDecompressionStatistics() const override;

    bool IsFetchingCompleted() const override;

    std::vector<NChunkClient::TChunkId> GetFailedChunkIds() const override;

protected:
    const NChunkClient::TBlockFetcherConfigPtr Config_;
    const NChunkClient::IBlockCachePtr BlockCache_;
    const NChunkClient::IChunkReaderPtr UnderlyingReader_;
    const NChunkClient::TClientChunkReadOptions ChunkReadOptions_;

    NChunkClient::TSequentialBlockFetcherPtr SequentialBlockFetcher_;
    NChunkClient::TChunkReaderMemoryManagerPtr MemoryManager_;
    TFuture<NChunkClient::TBlock> CurrentBlock_;

    bool BlockEnded_ = false;
    bool InitFirstBlockNeeded_ = false;
    bool InitNextBlockNeeded_ = false;

    bool CheckRowLimit_ = false;
    bool CheckKeyLimit_ = false;

    TChunkedMemoryPool MemoryPool_;

    NTracing::TTraceContextPtr TraceContext_;
    NTracing::TTraceContextFinishGuard FinishGuard_;

    bool BeginRead();
    bool OnBlockEnded();

    TFuture<void> DoOpen(
        std::vector<NChunkClient::TBlockFetcher::TBlockInfo> blockSequence,
        const NChunkClient::NProto::TMiscExt& miscExt,
        IInvokerPtr sessionInvoker = nullptr);

    //! Used in versioned chunk reader only and thus still uses legacy keys.
    void CheckBlockUpperKeyLimit(
        TLegacyKey blockLastKey,
        TLegacyKey upperLimit,
        int keyColumnCount,
        int commonKeyPrefix);

    void CheckBlockUpperLimits(
        i64 blockChunkRowCount,
        TUnversionedRow blockLastKey,
        const NChunkClient::TReadLimit& upperLimit,
        TRange<ESortOrder> sortOrders,
        int commonKeyPrefix);

    // These methods return min block index, satisfying the lower limit.
    int ApplyLowerRowLimit(
        const NProto::TDataBlockMetaExt& blockMeta,
        const NChunkClient::TReadLimit& lowerLimit) const;
    int ApplyLowerKeyLimit(
        const TSharedRange<TUnversionedRow>& blockLastKeys,
        const NChunkClient::TReadLimit& lowerLimit,
        TRange<ESortOrder> sortOrders,
        int commonKeyPrefix) const;

    // These methods return max block index, satisfying the upper limit.
    int ApplyUpperRowLimit(
        const NProto::TDataBlockMetaExt& blockMeta,
        const NChunkClient::TReadLimit& upperLimit) const;
    int ApplyUpperKeyLimit(
        const TSharedRange<TUnversionedRow>& blockLastKeys,
        const NChunkClient::TReadLimit& upperLimit,
        TRange<ESortOrder> sortOrders,
        int commonKeyPrefix) const;

    virtual void InitFirstBlock() = 0;
    virtual void InitNextBlock() = 0;

private:
    NLogging::TLogger Logger;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
