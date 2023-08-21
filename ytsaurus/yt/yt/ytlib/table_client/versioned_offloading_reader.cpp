#include "versioned_offloading_reader.h"
#include "tablet_snapshot.h"

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

class TVersionedOffloadingReader
    : public IVersionedReader
{
public:
    TVersionedOffloadingReader(
        IOffloadingReaderPtr offloadingReader,
        TOffloadingReaderOptionsPtr options,
        TSharedRange<TLegacyKey> lookupKeys)
        : OffloadingReader_(std::move(offloadingReader))
        , Options_(std::move(options))
        , LookupKeys_(std::move(lookupKeys))
        , WireFormatSchemaData_(
            IWireProtocolReader::GetSchemaData(*Options_->TableSchema))
    {
        DoOpen();
    }

    TFuture<void> Open() override
    {
        return GetReadyEvent();
    }

    IVersionedRowBatchPtr Read(const TRowBatchReadOptions& readOptions) override
    {
        YT_VERIFY(readOptions.MaxRowsPerRead > 0);

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
                readOptions.MaxRowsPerRead));

        while (rows.size() < rows.capacity()) {
            rows.push_back(WireReader_->ReadVersionedRow(WireFormatSchemaData_, /*captureValues*/ false));
            ExistingRowCount_ += static_cast<bool>(rows.back());
            DataWeight_ += GetDataWeight(rows.back());
        }

        RowCount_ += rows.size();

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
        dataStatistics.set_row_count(ExistingRowCount_);
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
    const IOffloadingReaderPtr OffloadingReader_;
    const TOffloadingReaderOptionsPtr Options_;
    const TSharedRange<TLegacyKey> LookupKeys_;

    const TSchemaData WireFormatSchemaData_;
    NCompression::ICodec* const Codec_ = NCompression::GetCodec(CompressionCodecId);

    const TRowBufferPtr RowBuffer_ = New<TRowBuffer>(TDataBufferTag());

    TFuture<void> ReadyEvent_;
    int RowCount_ = 0;
    int ExistingRowCount_ = 0;
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

        ReadyEvent_ = OffloadingReader_->LookupRows(
            Options_,
            LookupKeys_,
            ComputeEstimatedSize(),
            CompressionCodecId,
            GetCurrentInvoker())
            .Apply(BIND(&TVersionedOffloadingReader::OnRowsLookuped, MakeStrong(this)));
    }

    i64 ComputeEstimatedSize()
    {
        i64 estimatedSize = 0;

        int keyCount = Options_->TableSchema->GetKeyColumnCount();
        int valueCount = Options_->TableSchema->GetValueColumnCount();
        // Lower bound on row size is one write timestamp and zero delete timestamps.
        estimatedSize = GetVersionedRowByteSize(keyCount, valueCount, 1, 0);

        for (const auto& column : Options_->TableSchema->Columns()) {
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

IVersionedReaderPtr CreateVersionedOffloadingLookupReader(
    IOffloadingReaderPtr offloadingReader,
    TOffloadingReaderOptionsPtr options,
    TSharedRange<TLegacyKey> lookupKeys)
{
    return New<TVersionedOffloadingReader>(
        std::move(offloadingReader),
        std::move(options),
        std::move(lookupKeys));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
