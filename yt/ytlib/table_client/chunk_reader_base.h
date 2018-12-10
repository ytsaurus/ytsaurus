#pragma once

#include "chunk_meta_extensions.h"

#include <yt/client/chunk_client/reader_base.h>
#include <yt/client/chunk_client/read_limit.h>

#include <yt/ytlib/chunk_client/chunk_meta_extensions.h>
#include <yt/ytlib/chunk_client/block_fetcher.h>

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

class TChunkReaderBase
    : public virtual NChunkClient::IReaderBase
{
public:
    TChunkReaderBase(
        NChunkClient::TBlockFetcherConfigPtr config,
        NChunkClient::IChunkReaderPtr underlyingReader,
        NChunkClient::IBlockCachePtr blockCache,
        const NChunkClient::TClientBlockReadOptions& blockReadOptions);

    virtual TFuture<void> GetReadyEvent() override;

    virtual NChunkClient::NProto::TDataStatistics GetDataStatistics() const override;

    virtual NChunkClient::TCodecStatistics GetDecompressionStatistics() const override;

    virtual bool IsFetchingCompleted() const override;

    virtual std::vector<NChunkClient::TChunkId> GetFailedChunkIds() const override;

protected:
    const NChunkClient::TBlockFetcherConfigPtr Config_;
    const NChunkClient::IBlockCachePtr BlockCache_;
    const NChunkClient::IChunkReaderPtr UnderlyingReader_;
    const NChunkClient::TClientBlockReadOptions BlockReadOptions_;

    NChunkClient::TSequentialBlockFetcherPtr SequentialBlockFetcher_;
    NConcurrency::TAsyncSemaphorePtr AsyncSemaphore_;
    TFuture<void> ReadyEvent_ = VoidFuture;
    TFuture<NChunkClient::TBlock> CurrentBlock_;

    bool BlockEnded_ = false;
    bool InitFirstBlockNeeded_ = false;
    bool InitNextBlockNeeded_ = false;

    bool CheckRowLimit_ = false;
    bool CheckKeyLimit_ = false;

    TChunkedMemoryPool MemoryPool_;

    bool BeginRead();
    bool OnBlockEnded();

    TFuture<void> DoOpen(
        std::vector<NChunkClient::TBlockFetcher::TBlockInfo> blockSequence,
        const NChunkClient::NProto::TMiscExt& miscExt);

    int GetBlockIndexByKey(
        TKey key,
        const TSharedRange<TKey>& blockIndexKeys,
        std::optional<int> keyColumnCount) const;

    void CheckBlockUpperKeyLimit(
        TKey blockLastKey,
        TKey upperLimit,
        std::optional<int> keyColumnCount = std::nullopt);

    void CheckBlockUpperLimits(
        i64 blockChunkRowCount,
        TKey blockLastKey,
        const NChunkClient::TReadLimit& upperLimit,
        std::optional<int> keyColumnCount = std::nullopt);

    // These methods return min block index, satisfying the lower limit.
    int ApplyLowerRowLimit(const NProto::TBlockMetaExt& blockMeta, const NChunkClient::TReadLimit& lowerLimit) const;
    int ApplyLowerKeyLimit(const TSharedRange<TKey>& blockIndexKeys, const NChunkClient::TReadLimit& lowerLimit, std::optional<int> keyColumnCount = std::nullopt) const;

    // These methods return max block index, satisfying the upper limit.
    int ApplyUpperRowLimit(const NProto::TBlockMetaExt& blockMeta, const NChunkClient::TReadLimit& upperLimit) const;
    int ApplyUpperKeyLimit(const TSharedRange<TKey>& blockIndexKeys, const NChunkClient::TReadLimit& upperLimit, std::optional<int> keyColumnCount = std::nullopt) const;

    virtual void InitFirstBlock() = 0;
    virtual void InitNextBlock() = 0;

private:
    NLogging::TLogger Logger;

    TKey WidenKey(const TKey& key, std::optional<int> keyColumnCount, TChunkedMemoryPool* pool) const;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
