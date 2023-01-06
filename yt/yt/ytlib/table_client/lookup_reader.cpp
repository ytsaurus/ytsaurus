#include "lookup_reader.h"
#include "tablet_snapshot.h"

#include <yt/yt/ytlib/chunk_client/chunk_reader.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader_options.h>

#include <yt/yt/client/chunk_client/reader_base.h>
#include <yt/yt_proto/yt/client/chunk_client/proto/data_statistics.pb.h>

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

class TVersionedLookupReader
    : public IVersionedReader
{
public:
    TVersionedLookupReader(
        ILookupReaderPtr lookupReader,
        TClientChunkReadOptions chunkReadOptions,
        TSharedRange<TLegacyKey> lookupKeys,
        TTabletSnapshotPtr tabletSnapshot,
        TColumnFilter columnFilter,
        TTimestamp timestamp,
        bool produceAllVersions,
        TTimestamp overrideTimestamp)
        : LookupReader_(std::move(lookupReader))
        , RowsReadOptions_(std::move(chunkReadOptions))
        , LookupKeys_(std::move(lookupKeys))
        , TabletSnapshot_(std::move(tabletSnapshot))
        , ColumnFilter_(std::move(columnFilter))
        , SchemaData_(IWireProtocolReader::GetSchemaData(*TabletSnapshot_->TableSchema))
        , Timestamp_(timestamp)
        , ProduceAllVersions_(produceAllVersions)
        , OverrideTimestamp_(overrideTimestamp)
    {
        DoOpen();
    }

    TFuture<void> Open() override
    {
        return GetReadyEvent();
    }

    IVersionedRowBatchPtr Read(const TRowBatchReadOptions& options) override
    {
        YT_VERIFY(options.MaxRowsPerRead > 0);

        if (!ReadyEvent_.IsSet() || !ReadyEvent_.Get().IsOK()) {
            return CreateEmptyVersionedRowBatch();
        }

        if (RowCount_ == std::ssize(LookupKeys_)) {
            return nullptr;
        }

        RowBuffer_->Clear();

        std::vector<TVersionedRow> rows;
        rows.reserve(
            std::min(
                std::ssize(LookupKeys_) - RowCount_,
                options.MaxRowsPerRead));

        while (rows.size() < rows.capacity()) {
            ++RowCount_;
            rows.push_back(WireReader_->ReadVersionedRow(SchemaData_, /*captureValues*/ false));
            DataWeight_ += GetDataWeight(rows.back());
        }

        return CreateBatchFromVersionedRows(MakeSharedRange(std::move(rows), MakeStrong(this)));
    }

    TFuture<void> GetReadyEvent() const override
    {
        return ReadyEvent_;
    }

    NChunkClient::NProto::TDataStatistics GetDataStatistics() const override
    {
        NChunkClient::NProto::TDataStatistics dataStatistics;
        dataStatistics.set_chunk_count(1);
        dataStatistics.set_compressed_data_size(CompressedDataSize_.load());
        dataStatistics.set_uncompressed_data_size(UncompressedDataSize_.load());
        dataStatistics.set_row_count(RowCount_);
        dataStatistics.set_data_weight(DataWeight_);
        return dataStatistics;
    }

    TCodecStatistics GetDecompressionStatistics() const override
    {
        TCodecDuration decompressionTime{
            CompressionCodecId,
            NProfiling::ValueToDuration(DecompressionTime_.load())};
        return TCodecStatistics().Append(decompressionTime);
    }

    bool IsFetchingCompleted() const override
    {
        YT_ABORT();
    }

    std::vector<TChunkId> GetFailedChunkIds() const override
    {
        // TODO(akozhikhov): get chunk id here.
        return {};
    }

private:
    const ILookupReaderPtr LookupReader_;
    const TClientChunkReadOptions RowsReadOptions_;
    const TSharedRange<TLegacyKey> LookupKeys_;
    const TTabletSnapshotPtr TabletSnapshot_;
    const TColumnFilter ColumnFilter_;
    const TSchemaData SchemaData_;
    const TTimestamp Timestamp_;
    const bool ProduceAllVersions_;
    const TTimestamp OverrideTimestamp_;
    NCompression::ICodec* const Codec_ = NCompression::GetCodec(CompressionCodecId);

    const TRowBufferPtr RowBuffer_ = New<TRowBuffer>(TDataBufferTag());

    TFuture<void> ReadyEvent_;
    int RowCount_ = 0;
    i64 DataWeight_ = 0;
    std::atomic<i64> CompressedDataSize_ = 0;
    std::atomic<i64> UncompressedDataSize_ = 0;
    std::atomic<NProfiling::TCpuDuration> DecompressionTime_ = 0;

    std::unique_ptr<IWireProtocolReader> WireReader_;


    void DoOpen()
    {
        if (LookupKeys_.Empty()) {
            ReadyEvent_ = VoidFuture;
            return;
        }

        ReadyEvent_ = LookupReader_->LookupRows(
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
            OverrideTimestamp_,
            GetCurrentInvoker())
            .Apply(BIND(&TVersionedLookupReader::OnRowsLookuped, MakeStrong(this)));
    }

    i64 ComputeEstimatedSize()
    {
        i64 estimatedSize = 0;

        int keyCount = TabletSnapshot_->TableSchema->GetKeyColumnCount();
        int valueCount = TabletSnapshot_->TableSchema->GetValueColumnCount();
        // Lower bound on row size is one write timestamp and zero delete timestamps.
        estimatedSize = GetVersionedRowByteSize(keyCount, valueCount, 1, 0);

        for (const auto& column : TabletSnapshot_->TableSchema->Columns()) {
            if (IsStringLikeType(column.GetWireType())) {
                estimatedSize += ExpectedStringSize - sizeof(TUnversionedValueData);
            }
        }

        // Here we don't count chunk_reader_statistics size which also comes in TRspLookup protobuf.
        return estimatedSize * LookupKeys_.Size();
    }

    void OnRowsLookuped(const TSharedRef& compressedRowset)
    {
        CompressedDataSize_ += compressedRowset.Size();

        TWallTimer timer;
        auto rowset = Codec_->Decompress(compressedRowset);
        DecompressionTime_ += timer.GetElapsedValue();
        UncompressedDataSize_ += rowset.Size();

        WireReader_ = CreateWireProtocolReader(std::move(rowset), RowBuffer_);
    }
};

////////////////////////////////////////////////////////////////////////////////

IVersionedReaderPtr CreateVersionedLookupReader(
    ILookupReaderPtr lookupReader,
    TClientChunkReadOptions chunkReadOptions,
    TSharedRange<TLegacyKey> lookupKeys,
    TTabletSnapshotPtr tabletSnapshot,
    TColumnFilter columnFilter,
    TTimestamp timestamp,
    bool produceAllVersions,
    TTimestamp overrideTimestamp)
{
    return New<TVersionedLookupReader>(
        std::move(lookupReader),
        std::move(chunkReadOptions),
        std::move(lookupKeys),
        std::move(tabletSnapshot),
        std::move(columnFilter),
        timestamp,
        produceAllVersions,
        overrideTimestamp);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
