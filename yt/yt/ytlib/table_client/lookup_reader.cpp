#include "lookup_reader.h"
#include "tablet_snapshot.h"

#include <yt/yt/ytlib/chunk_client/chunk_reader.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader_options.h>

#include <yt/yt/client/chunk_client/reader_base.h>
#include <yt/yt/client/chunk_client/proto/data_statistics.pb.h>

#include <yt/yt/client/table_client/row_buffer.h>
#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/versioned_reader.h>
#include <yt/yt/client/table_client/wire_protocol.h>

#include <yt/yt/core/compression/codec.h>

#include <yt/yt/core/profiling/timing.h>

namespace NYT::NTableClient {

using namespace NChunkClient;
using namespace NProfiling;

////////////////////////////////////////////////////////////////////////////////

// TODO(akozhikhov): Consider keeping dynamic statistic for this parameter instead.
static constexpr i64 ExpectedStringSize = 256;
static constexpr auto CompressionCodecId = NCompression::ECodec::Lz4;

struct TDataBufferTag { };

////////////////////////////////////////////////////////////////////////////////

class TRowLookupReader
    : public IVersionedReader
{
public:
    TRowLookupReader(
        ILookupReaderPtr underlyingReader,
        TClientChunkReadOptions chunkReadOptions,
        TSharedRange<TLegacyKey> lookupKeys,
        TTabletSnapshotPtr tabletSnapshot,
        TColumnFilter columnFilter,
        TTimestamp timestamp,
        bool produceAllVersions,
        TTimestamp chunkTimestamp,
        bool enablePeerProbing,
        bool enableRejectsIfThrottling)
        : UnderlyingReader_(std::move(underlyingReader))
        , RowsReadOptions_(std::move(chunkReadOptions))
        , LookupKeys_(std::move(lookupKeys))
        , TabletSnapshot_(std::move(tabletSnapshot))
        , ColumnFilter_(std::move(columnFilter))
        , Timestamp_(timestamp)
        , ProduceAllVersions_(produceAllVersions)
        , ChunkTimestamp_(chunkTimestamp)
        , EnablePeerProbing_(enablePeerProbing)
        , EnableRejectsIfThrottling_(enableRejectsIfThrottling)
    {
        ReadyEvent_ = DoOpen();
    }

    virtual TFuture<void> Open() override
    {
        return GetReadyEvent();
    }

    virtual IVersionedRowBatchPtr Read(const TRowBatchReadOptions& options) override
    {
        YT_VERIFY(options.MaxRowsPerRead > 0);

        if (!BeginRead()) {
            return CreateEmptyVersionedRowBatch();
        }

        if (RowCount_ == LookupKeys_.size()) {
            return nullptr;
        }

        YT_VERIFY(FetchedRows_.size() == LookupKeys_.size());

        std::vector<TVersionedRow> rows;
        int batchSize = std::min(
            options.MaxRowsPerRead,
            static_cast<i64>(LookupKeys_.size()) - RowCount_);
        rows.reserve(batchSize);

        for (int index = 0; index < batchSize; ++index) {
            rows.push_back(FetchedRows_[RowCount_++]);
            DataWeight_ += GetDataWeight(rows.back());
        }

        return CreateBatchFromVersionedRows(MakeSharedRange(rows, MakeStrong(this)));
    }

    virtual TFuture<void> GetReadyEvent() const override
    {
        return ReadyEvent_;
    }

    virtual NChunkClient::NProto::TDataStatistics GetDataStatistics() const override
    {
        NChunkClient::NProto::TDataStatistics dataStatistics;
        dataStatistics.set_chunk_count(1);
        dataStatistics.set_compressed_data_size(CompressedDataSize_.load());
        dataStatistics.set_uncompressed_data_size(UncompressedDataSize_.load());
        dataStatistics.set_row_count(RowCount_);
        dataStatistics.set_data_weight(DataWeight_);
        return dataStatistics;
    }

    virtual TCodecStatistics GetDecompressionStatistics() const override
    {
        TCodecDuration decompressionTime{
            CompressionCodecId,
            NProfiling::ValueToDuration(DecompressionTime_.load())};
        return TCodecStatistics().Append(decompressionTime);
    }

    virtual bool IsFetchingCompleted() const override
    {
        YT_ABORT();
    }

    virtual std::vector<TChunkId> GetFailedChunkIds() const override
    {
        // TODO(akozhikhov): get chunk id here.
        return {};
    }

private:
    const ILookupReaderPtr UnderlyingReader_;
    const TClientChunkReadOptions RowsReadOptions_;
    const TSharedRange<TLegacyKey> LookupKeys_;
    const TTabletSnapshotPtr TabletSnapshot_;
    const TColumnFilter ColumnFilter_;
    const TTimestamp Timestamp_;
    const bool ProduceAllVersions_;
    const TTimestamp ChunkTimestamp_;
    NCompression::ICodec* const Codec_ = NCompression::GetCodec(CompressionCodecId);
    const bool EnablePeerProbing_;
    const bool EnableRejectsIfThrottling_;

    const TRowBufferPtr RowBuffer_ = New<TRowBuffer>(TDataBufferTag());

    TFuture<void> ReadyEvent_ = VoidFuture;
    int RowCount_ = 0;
    i64 DataWeight_ = 0;
    std::atomic<i64> CompressedDataSize_ = 0;
    std::atomic<i64> UncompressedDataSize_ = 0;
    std::atomic<NProfiling::TCpuDuration> DecompressionTime_ = 0;

    TFuture<TSharedRef> FetchedRowset_;
    std::vector<TVersionedRow> FetchedRows_;

    TFuture<void> DoOpen()
    {
        if (LookupKeys_.Empty()) {
            return VoidFuture;
        }

        FetchedRowset_ = UnderlyingReader_->LookupRows(
            RowsReadOptions_,
            LookupKeys_,
            TabletSnapshot_->TableId,
            TabletSnapshot_->MountRevision,
            TabletSnapshot_->TableSchema,
            ComputeEstimatedSize(),
            ColumnFilter_,
            Timestamp_,
            CompressionCodecId,
            ProduceAllVersions_,
            ChunkTimestamp_,
            EnablePeerProbing_,
            EnableRejectsIfThrottling_)
            .Apply(BIND([=, this_ = MakeStrong(this)] (const TSharedRef& fetchedRowset) {
                ProcessFetchedRowset(fetchedRowset);
                return fetchedRowset;
            }));
        return FetchedRowset_.As<void>();
    }

    i64 ComputeEstimatedSize()
    {
        i64 estimatedSize = 0;

        int keyCount = TabletSnapshot_->TableSchema->GetKeyColumnCount();
        int valueCount = TabletSnapshot_->TableSchema->GetValueColumnCount();
        // Lower bound on row size is one write timestamp and zero delete timestamps.
        estimatedSize = GetVersionedRowByteSize(keyCount, valueCount, 1, 0);

        for (const auto& column : TabletSnapshot_->TableSchema->Columns()) {
            if (IsStringLikeType(column.GetPhysicalType())) {
                estimatedSize += ExpectedStringSize - sizeof(TUnversionedValueData);
            }
        }

        // Here we don't count chunk_reader_statistics size which also comes in TRspLookup protobuf.
        return estimatedSize * LookupKeys_.Size();
    }

    void ProcessFetchedRowset(const TSharedRef& fetchedRowset)
    {
        CompressedDataSize_ += fetchedRowset.Size();

        TWallTimer timer;
        auto uncompressedFetchedRowset = Codec_->Decompress(fetchedRowset);
        DecompressionTime_ += timer.GetElapsedValue();
        UncompressedDataSize_ += uncompressedFetchedRowset.Size();

        auto schemaData = TWireProtocolReader::GetSchemaData(*TabletSnapshot_->TableSchema, TColumnFilter());
        TWireProtocolReader reader(uncompressedFetchedRowset, RowBuffer_);

        FetchedRows_.reserve(LookupKeys_.Size());
        for (int i = 0; i < LookupKeys_.Size(); ++i) {
            FetchedRows_.push_back(reader.ReadVersionedRow(schemaData, true));
        }
    }

    bool BeginRead()
    {
        if (!FetchedRowset_.IsSet()) {
            return false;
        }

        if (!FetchedRowset_.Get().IsOK()) {
            return false;
        }

        return true;
    }
};

////////////////////////////////////////////////////////////////////////////////

IVersionedReaderPtr CreateRowLookupReader(
    ILookupReaderPtr underlyingReader,
    TClientChunkReadOptions chunkReadOptions,
    TSharedRange<TLegacyKey> lookupKeys,
    TTabletSnapshotPtr tabletSnapshot,
    TColumnFilter columnFilter,
    TTimestamp timestamp,
    bool produceAllVersions,
    TTimestamp chunkTimestamp,
    bool enablePeerProbing,
    bool enableRejectsIfThrottling)
{
    return New<TRowLookupReader>(
        std::move(underlyingReader),
        std::move(chunkReadOptions),
        std::move(lookupKeys),
        std::move(tabletSnapshot),
        std::move(columnFilter),
        timestamp,
        produceAllVersions,
        chunkTimestamp,
        enablePeerProbing,
        enableRejectsIfThrottling);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
