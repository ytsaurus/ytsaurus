#pragma once

#include "chunk_meta_extensions.h"
#include "timing_reader.h"

#include <yt/yt/ytlib/chunk_client/public.h>
#include <yt/yt/ytlib/chunk_client/block_fetcher.h>
#include <yt/yt/ytlib/chunk_client/chunk_meta_extensions.h>
#include <yt/yt/ytlib/chunk_client/block.h>
#include <yt/yt/ytlib/chunk_client/data_source.h>

#include <yt/yt/ytlib/table_chunk_format/column_reader.h>

#include <yt/yt_proto/yt/client/chunk_client/proto/data_statistics.pb.h>
#include <yt/yt/client/chunk_client/reader_base.h>
#include <yt/yt/client/chunk_client/read_limit.h>

#include <yt/yt/client/table_client/column_sort_schema.h>

#include <yt/yt/library/random/bernoulli_sampler.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

class TColumnarChunkReaderBase
    : public virtual NChunkClient::IReaderBase
    , public TTimingReaderBase
{
public:
    TColumnarChunkReaderBase(
        TColumnarChunkMetaPtr chunkMeta,
        const std::optional<NChunkClient::TDataSource>& dataSource,
        TChunkReaderConfigPtr config,
        NChunkClient::IChunkReaderPtr underlyingReader,
        TRange<ESortOrder> sortOrders,
        int commonKeyPrefix,
        NChunkClient::IBlockCachePtr blockCache,
        const NChunkClient::TClientChunkReadOptions& chunkReadOptions,
        std::function<void(int /*skippedRowCount*/)> onRowsSkipped,
        const NChunkClient::TChunkReaderMemoryManagerHolderPtr& memoryManagerHolder = nullptr);

protected:
    const TColumnarChunkMetaPtr ChunkMeta_;
    const TChunkReaderConfigPtr Config_;
    const NChunkClient::IChunkReaderPtr UnderlyingReader_;
    const NChunkClient::IBlockCachePtr BlockCache_;
    const NChunkClient::TClientChunkReadOptions ChunkReadOptions_;

    const std::vector<ESortOrder> SortOrders_;
    const int CommonKeyPrefix_;

    TBernoulliSampler Sampler_;

    std::function<void(int)> OnRowsSkipped_;

    NChunkClient::TChunkReaderMemoryManagerHolderPtr MemoryManagerHolder_;
    NChunkClient::TBlockFetcherPtr BlockFetcher_;

    std::vector<TFuture<NChunkClient::TBlock>> PendingBlocks_;

    i64 RequiredMemorySize_ = 0;

    struct TColumn
    {
        TColumn(std::unique_ptr<NTableChunkFormat::IColumnReaderBase> reader, int columnMetaIndex = -1, int columnId = -1)
            : ColumnReader(std::move(reader))
            , ColumnMetaIndex(columnMetaIndex)
            , ColumnId(columnId)
        { }

        std::unique_ptr<NTableChunkFormat::IColumnReaderBase> ColumnReader;
        int ColumnMetaIndex;
        int ColumnId;
        std::vector<int> BlockIndexSequence;
        int PendingBlockIndex = 0;
    };

    std::vector<TColumn> Columns_;

    std::optional<int> SampledColumnIndex_;
    std::vector<NChunkClient::TReadRange> SampledRanges_;
    int SampledRangeIndex_ = 0;
    bool SampledRangeIndexChanged_ = false;

    bool IsSamplingCompleted_ = false;

    NTracing::TTraceContextPtr TraceContext_;
    NTracing::TTraceContextFinishGuard FinishGuard_;

    NChunkClient::NProto::TDataStatistics GetDataStatistics() const override;
    NChunkClient::TCodecStatistics GetDecompressionStatistics() const override;
    bool IsFetchingCompleted() const override;
    std::vector<NChunkClient::TChunkId> GetFailedChunkIds() const override;

    void FeedBlocksToReaders();
    void ArmColumnReaders();
    i64 GetReadyRowCount() const;

    NChunkClient::TBlockFetcher::TBlockInfo CreateBlockInfo(int blockIndex) const;
    i64 GetSegmentIndex(const TColumn& column, i64 rowIndex) const;
    i64 GetLowerKeyBoundIndex(TKeyBound lowerBound) const;

    //! Returns |true| if block sampling is enabled and all sampling ranges have been read.
    bool IsSamplingCompleted() const;
};

////////////////////////////////////////////////////////////////////////////////

class TColumnarRangeChunkReaderBase
    : public TColumnarChunkReaderBase
{
public:
    using TColumnarChunkReaderBase::TColumnarChunkReaderBase;

protected:
    NChunkClient::TReadLimit LowerLimit_;
    NChunkClient::TReadLimit UpperLimit_;

    // Lower limit (both, key and row index) is greater or equal than this row index.
    // No need to read and check keys with lesser row indexes.
    i64 LowerRowIndex_;

    // Upper limit (both, key and row index) is greater or equal than this row index.
    // No need to check keys below this row index.
    i64 SafeUpperRowIndex_;

    // Upper limit (both, key and row index) is less or equal than this row index.
    // We should check UpperLimit_.GetKey() between SafeUpperRowIndex and HardUpperRowIndex.
    i64 HardUpperRowIndex_;

    void InitLowerRowIndex();
    void InitUpperRowIndex();

    void Initialize(TRange<NTableChunkFormat::IUnversionedColumnReader*> keyReaders);

    void InitBlockFetcher(IInvokerPtr sessionInvoker = nullptr);
    TFuture<void> RequestFirstBlocks();

    bool TryFetchNextRow();
};

////////////////////////////////////////////////////////////////////////////////

class TColumnarLookupChunkReaderBase
    : public TColumnarChunkReaderBase
{
public:
    using TColumnarChunkReaderBase::TColumnarChunkReaderBase;

protected:
    TSharedRange<TLegacyKey> Keys_;
    std::vector<i64> RowIndexes_;
    i64 NextKeyIndex_ = 0;

    void Initialize();
    void InitBlockFetcher();
    TFuture<void> RequestFirstBlocks();

    bool TryFetchNextRow();
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
