#include "table_reader.h"
#include "helpers.h"

#include <yt/client/api/rowset.h>
#include <yt/client/api/table_reader.h>

#include <yt/client/chunk_client/proto/data_statistics.pb.h>
#include <yt/client/table_client/name_table.h>

#include <yt/core/rpc/stream.h>

namespace NYT::NApi::NRpcProxy {

using namespace NConcurrency;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

class TTableReader
    : public ITableReader
{
public:
    TTableReader(
        IAsyncZeroCopyInputStreamPtr underlying,
        i64 startRowIndex,
        const TKeyColumns& keyColumns,
        const std::vector<TString>& omittedInaccessibleColumns,
        const TTableSchema& schema,
        const NApi::NRpcProxy::NProto::TTableReaderPayload& payload)
        : Underlying_ (std::move(underlying))
        , StartRowIndex_(startRowIndex)
        , KeyColumns_(keyColumns)
        , TableSchema_(schema)
        , OmittedInaccessibleColumns_(omittedInaccessibleColumns)
    {
        YT_VERIFY(Underlying_);
        RowsetDescriptor_.set_wire_format_version(NApi::NRpcProxy::CurrentWireFormatVersion);
        RowsetDescriptor_.set_rowset_kind(NApi::NRpcProxy::NProto::RK_UNVERSIONED);

        ApplyReaderPayload(payload);

        AsyncRowsWithPayload_ = GetRowsWithPayload();
        ReadyEvent_.TrySetFrom(AsyncRowsWithPayload_);
    }

    virtual i64 GetStartRowIndex() const override
    {
        return StartRowIndex_;
    }

    virtual i64 GetTotalRowCount() const override
    {
        return TotalRowCount_;
    }

    virtual NChunkClient::NProto::TDataStatistics GetDataStatistics() const override
    {
        auto dataStatistics = DataStatistics_;
        dataStatistics.set_row_count(RowCount_);
        dataStatistics.set_data_weight(DataWeight_);

        return dataStatistics;
    }

    virtual TFuture<void> GetReadyEvent() override
    {
        return ReadyEvent_;
    }

    virtual bool Read(std::vector<TUnversionedRow>* rows) override
    {
        YT_VERIFY(rows->capacity() > 0);
        rows->clear();
        StoredRows_.clear();

        if (!ReadyEvent_.IsSet() || !ReadyEvent_.Get().IsOK()) {
            return true;
        }

        if (!Finished_) {
            ReadyEvent_ = NewPromise<void>();
        }

        while (AsyncRowsWithPayload_ && AsyncRowsWithPayload_.IsSet() && AsyncRowsWithPayload_.Get().IsOK() &&
            !Finished_ && rows->size() < rows->capacity())
        {
            const auto& currentRows = AsyncRowsWithPayload_.Get().Value().Rows;
            const auto& currentPayload = AsyncRowsWithPayload_.Get().Value().Payload;

            if (currentRows.Empty()) {
                ReadyEvent_.Set();
                Finished_ = true;
                ApplyReaderPayload(currentPayload);
                continue;
            }
            i64 readRowsCount = std::min(
                rows->capacity() - rows->size(),
                currentRows.Size() - CurrentRowsOffset_);
            RowCount_ += readRowsCount;
            i64 newOffset = CurrentRowsOffset_ + readRowsCount;
            for (i64 rowIndex = CurrentRowsOffset_; rowIndex < newOffset; ++rowIndex) {
                rows->push_back(currentRows[rowIndex]);
                DataWeight_ += GetDataWeight(currentRows[rowIndex]);
            }
            CurrentRowsOffset_ = newOffset;

            StoredRows_.push_back(currentRows);
            ApplyReaderPayload(currentPayload);

            if (CurrentRowsOffset_ == currentRows.size()) {
                AsyncRowsWithPayload_ = GetRowsWithPayload();
                CurrentRowsOffset_ = 0;
            }
        }

        ReadyEvent_ .TrySetFrom(AsyncRowsWithPayload_);
        return !rows->empty();
    }

    virtual const TNameTablePtr& GetNameTable() const override
    {
        return NameTable_;
    }

    virtual const TKeyColumns& GetKeyColumns() const override
    {
        return KeyColumns_;
    }

    virtual const TTableSchema& GetTableSchema() const override
    {
        return TableSchema_;
    }

    virtual const std::vector<TString>& GetOmittedInaccessibleColumns() const override
    {
        return OmittedInaccessibleColumns_;
    }

private:
    using TRows = TSharedRange<TUnversionedRow>;
    using TPayload = NApi::NRpcProxy::NProto::TTableReaderPayload;
    struct TRowsWithPayload
    {
        TRows Rows;
        TPayload Payload;
    };

    const IAsyncZeroCopyInputStreamPtr Underlying_;
    const i64 StartRowIndex_;
    const TKeyColumns KeyColumns_;
    const TTableSchema TableSchema_;
    const std::vector<TString> OmittedInaccessibleColumns_;
    const TNameTablePtr NameTable_ = New<TNameTable>();

    NChunkClient::NProto::TDataStatistics DataStatistics_;
    i64 TotalRowCount_;

    i64 RowCount_ = 0;
    size_t DataWeight_ = 0;

    TNameTableToSchemaIdMapping IdMapping_;
    NApi::NRpcProxy::NProto::TRowsetDescriptor RowsetDescriptor_;

    TPromise<void> ReadyEvent_ = NewPromise<void>();

    std::vector<TRows> StoredRows_;
    TFuture<TRowsWithPayload> AsyncRowsWithPayload_;
    i64 CurrentRowsOffset_ = 0;

    bool Finished_ = false;

    void ApplyReaderPayload(const TPayload& payload)
    {
        TotalRowCount_ = payload.total_row_count();
        DataStatistics_ = payload.data_statistics();
    }

    TFuture<TRowsWithPayload> GetRowsWithPayload()
    {
        return Underlying_->Read()
            .Apply(BIND([this, weakThis = MakeWeak(this)] (const TSharedRef& rowData) {
                auto this_ = weakThis.Lock();
                if (!this_) {
                    THROW_ERROR_EXCEPTION("Reader abandoned");
                }

                auto rowsWithPayload = DeserializeRows(rowData);
                if (rowsWithPayload.Rows.Empty()) {
                    return ExpectEndOfStream(Underlying_).Apply(BIND([=] () {
                        return std::move(rowsWithPayload);
                    }));
                }
                return MakeFuture(std::move(rowsWithPayload));
            }));
    }

    TRowsWithPayload DeserializeRows(const TSharedRef& rowData)
    {
        std::vector<TSharedRef> parts;
        UnpackRefsOrThrow(rowData, &parts);
        if (parts.size() != 2) {
            THROW_ERROR_EXCEPTION(
                "Error deserializing rows in table reader: expected %v packed refs, got %v",
                2,
                parts.size());
        }

        const auto& rowsetData = parts[0];
        const auto& payloadRef = parts[1];

        auto rows = DeserializeRowsetWithNameTableDelta(
            rowsetData,
            NameTable_,
            &RowsetDescriptor_,
            &IdMapping_);

        TPayload payload;
        if (!TryDeserializeProto(&payload, payloadRef)) {
            THROW_ERROR_EXCEPTION("Error deserializing table reader payload");
        }

        return {std::move(rows), std::move(payload)};
    }
};

TFuture<ITableReaderPtr> CreateTableReader(
    TApiServiceProxy::TReqReadTablePtr request)
{
    return NRpc::CreateRpcClientInputStream(std::move(request))
        .Apply(BIND([=] (const IAsyncZeroCopyInputStreamPtr& inputStream) {
            return inputStream->Read().Apply(BIND([=] (const TSharedRef& metaRef) {
                NApi::NRpcProxy::NProto::TReadTableMeta meta;
                if (!TryDeserializeProto(&meta, metaRef)) {
                    THROW_ERROR_EXCEPTION("Failed to deserialize table reader meta information");
                }

                i64 startRowIndex = meta.start_row_index();
                auto keyColumns = FromProto<TKeyColumns>(meta.key_columns());
                auto omittedInaccessibleColumns = FromProto<std::vector<TString>>(
                    meta.omitted_inaccessible_columns());
                auto schema = FromProto<TTableSchema>(meta.schema());

                return New<TTableReader>(
                    inputStream,
                    startRowIndex,
                    std::move(keyColumns),
                    std::move(omittedInaccessibleColumns),
                    std::move(schema),
                    meta.payload());
            })).As<ITableReaderPtr>();
        }));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NRpcProxy
