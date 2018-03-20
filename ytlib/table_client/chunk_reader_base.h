#pragma once

#include "public.h"
#include "chunk_meta_extensions.h"

#include <yt/ytlib/chunk_client/chunk_meta_extensions.h>
#include <yt/ytlib/chunk_client/reader_base.h>
#include <yt/ytlib/chunk_client/public.h>
#include <yt/ytlib/chunk_client/read_limit.h>
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
        const NChunkClient::TReadSessionId& sessionId);

    virtual TFuture<void> GetReadyEvent() override;

    virtual NChunkClient::NProto::TDataStatistics GetDataStatistics() const override;

    virtual NChunkClient::TCodecStatistics GetDecompressionStatistics() const override;

    virtual bool IsFetchingCompleted() const override;

    virtual std::vector<NChunkClient::TChunkId> GetFailedChunkIds() const override;

protected:
    const NChunkClient::TBlockFetcherConfigPtr Config_;
    const NChunkClient::IBlockCachePtr BlockCache_;
    const NChunkClient::IChunkReaderPtr UnderlyingReader_;
    const NChunkClient::TReadSessionId ReadSessionId_;

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

    static int GetBlockIndexByKey(
        TKey key,
        const TSharedRange<TKey>& blockIndexKeys,
        int beginBlockIndex = 0);

    void CheckBlockUpperKeyLimit(
        const NProto::TBlockMeta& blockMeta,
        TKey upperLimit,
        TNullable<int> keyColumnCount = Null);

    void CheckBlockUpperLimits(
        const NProto::TBlockMeta& blockMeta,
        const NChunkClient::TReadLimit& upperLimit,
        TNullable<int> keyColumnCount = Null);

    // These methods return min block index, satisfying the lower limit.
    int ApplyLowerRowLimit(const NProto::TBlockMetaExt& blockMeta, const NChunkClient::TReadLimit& lowerLimit) const;
    int ApplyLowerKeyLimit(const NProto::TBlockMetaExt& blockMeta, const NChunkClient::TReadLimit& lowerLimit, TNullable<int> keyColumnCount = Null) const;
    int ApplyLowerKeyLimit(const TSharedRange<TKey>& blockIndexKeys, const NChunkClient::TReadLimit& lowerLimit) const;

    // These methods return max block index, satisfying the upper limit.
    int ApplyUpperRowLimit(const NProto::TBlockMetaExt& blockMeta, const NChunkClient::TReadLimit& upperLimit) const;
    int ApplyUpperKeyLimit(const NProto::TBlockMetaExt& blockMeta, const NChunkClient::TReadLimit& upperLimit, TNullable<int> keyColumnCount = Null) const;
    int ApplyUpperKeyLimit(const TSharedRange<TKey>& blockIndexKeys, const NChunkClient::TReadLimit& upperLimit) const;

    virtual void InitFirstBlock() = 0;
    virtual void InitNextBlock() = 0;

private:
    NLogging::TLogger Logger;

    std::vector<TUnversionedValue> WidenKey(const TOwningKey& key, int keyColumnCount) const;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
