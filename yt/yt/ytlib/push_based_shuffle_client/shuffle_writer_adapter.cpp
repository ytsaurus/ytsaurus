#include "shuffle_writer_adapter.h"

#include "shuffle_writer.h"

#include <yt/yt/ytlib/chunk_client/multi_chunk_writer.h>

#include <yt/yt/ytlib/table_client/schemaless_chunk_writer.h>

#include <yt/yt/client/chunk_client/data_statistics.h>

#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/row_buffer.h>
#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/unversioned_row.h>

#include <yt/yt/core/actions/bind.h>
#include <yt/yt/core/actions/future.h>

namespace NYT::NPushBasedShuffleClient {

using namespace NChunkClient;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////

class TShuffleWriterAdapter
    : public ISchemalessMultiChunkWriter
{
public:
    TShuffleWriterAdapter(
        IPushBasedShuffleWriterPtr underlyingWriter,
        TTableSchemaPtr schema)
        : UnderlyingWriter_(std::move(underlyingWriter))
        , Schema_(std::move(schema))
        , NameTable_(TNameTable::FromSchema(*Schema_))
    { }

    bool Write(TRange<TUnversionedRow> rows) override
    {
        if (ReadyEvent_.IsSet() && !ReadyEvent_.GetOrCrash().IsOK()) {
            return false;
        }

        try {
            DoWrite(rows);
        } catch (const std::exception& ex) {
            // The interface reports a rejected batch through the ready event rather than
            // by throwing, and the failure stays there for every later call.
            ReadyEvent_ = MakeFuture(TError(ex));
            return false;
        }

        return ReadyEvent_.IsSet() && ReadyEvent_.GetOrCrash().IsOK();
    }

    TFuture<void> GetReadyEvent() override
    {
        return ReadyEvent_;
    }

    TFuture<void> Close() override
    {
        if (ReadyEvent_.IsSet() && !ReadyEvent_.GetOrCrash().IsOK()) {
            return ReadyEvent_;
        }

        return UnderlyingWriter_->Close();
    }

    const TNameTablePtr& GetNameTable() const override
    {
        return NameTable_;
    }

    const TTableSchemaPtr& GetSchema() const override
    {
        return Schema_;
    }

    std::optional<TRowsDigest> GetDigest() const override
    {
        return std::nullopt;
    }

    const std::vector<NChunkClient::NProto::TChunkSpec>& GetWrittenChunkSpecs() const override
    {
        static const std::vector<NChunkClient::NProto::TChunkSpec> ChunkSpecs;
        return ChunkSpecs;
    }

    const TWrittenChunkReplicasInfoList& GetWrittenChunkReplicasInfos() const override
    {
        static const TWrittenChunkReplicasInfoList ReplicasInfos;
        return ReplicasInfos;
    }

    NChunkClient::NProto::TDataStatistics GetDataStatistics() const override
    {
        NChunkClient::NProto::TDataStatistics dataStatistics;
        dataStatistics.set_row_count(RowCount_);
        dataStatistics.set_data_weight(DataWeight_);
        return dataStatistics;
    }

    TCodecStatistics GetCompressionStatistics() const override
    {
        return {};
    }

private:
    const IPushBasedShuffleWriterPtr UnderlyingWriter_;
    const TTableSchemaPtr Schema_;
    const TNameTablePtr NameTable_;
    const TRowBufferPtr RowBuffer_ = New<TRowBuffer>();

    TFuture<void> ReadyEvent_ = OKFuture;

    std::vector<bool> WrittenIds_;

    //! Logical mapper output: every row is counted once, no matter how many physical
    //! records carried it.
    std::atomic<i64> RowCount_ = 0;
    std::atomic<i64> DataWeight_ = 0;

    void DoWrite(TRange<TUnversionedRow> rows)
    {
        // The shuffle records are partitioned and decoded by value position, so every row
        // must carry the schema columns in schema order.
        RowBuffer_->Clear();
        auto schemafulRows = std::make_shared<std::vector<TUnversionedRow>>();
        schemafulRows->reserve(rows.size());

        int columnCount = Schema_->GetColumnCount();
        i64 dataWeight = 0;
        for (auto row : rows) {
            THROW_ERROR_EXCEPTION_IF(!row, "Unexpected null row");

            auto schemafulRow = RowBuffer_->AllocateUnversioned(columnCount);
            for (int index = 0; index < columnCount; ++index) {
                schemafulRow[index] = MakeUnversionedNullValue(index);
            }

            WrittenIds_.assign(columnCount, false);
            for (const auto& value : row) {
                THROW_ERROR_EXCEPTION_IF(
                    value.Id >= columnCount,
                    "Row value with id %v does not fit the shuffled schema with %v columns",
                    value.Id,
                    columnCount);
                THROW_ERROR_EXCEPTION_IF(
                    WrittenIds_[value.Id],
                    "Row contains duplicate values with id %v",
                    value.Id);

                WrittenIds_[value.Id] = true;
                schemafulRow[value.Id] = value;
            }

            // Nothing downstream re-checks the values: the record format addresses them by
            // position, and the sort reader compares whatever type arrived.
            for (int index = 0; index < columnCount; ++index) {
                ValidateValueType(
                    schemafulRow[index],
                    Schema_->Columns()[index],
                    /*typeAnyAcceptsAllValues*/ true);
            }

            // The shuffle writer consumes the batch asynchronously, so the row data may
            // not stay in the caller's memory.
            RowBuffer_->CaptureValues(schemafulRow);

            dataWeight += GetDataWeight(schemafulRow);
            schemafulRows->push_back(schemafulRow);
        }

        RowCount_ += std::ssize(*schemafulRows);
        DataWeight_ += dataWeight;

        // The shuffle writer reads the range on its own invoker after Write returns, so
        // the batch is owned by the ready event rather than by the adapter, which the
        // caller may drop with a write in flight.
        ReadyEvent_ = UnderlyingWriter_->Write(TRange(*schemafulRows))
            .Apply(BIND([rowBuffer = RowBuffer_, rows = schemafulRows] { }));
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace

////////////////////////////////////////////////////////////////////////////////

ISchemalessMultiChunkWriterPtr CreateShuffleWriterAdapter(
    IPushBasedShuffleWriterPtr underlyingWriter,
    TTableSchemaPtr schema)
{
    return New<TShuffleWriterAdapter>(
        std::move(underlyingWriter),
        std::move(schema));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NPushBasedShuffleClient
