#include "table_reader.h"

#include "helpers.h"

#include <yt/client/api/rowset.h>
#include <yt/client/api/table_reader.h>

#include <yt/client/chunk_client/proto/data_statistics.pb.h>
#include <yt/client/table_client/name_table.h>

#include <yt/client/table_client/row_buffer.h>

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
        const NApi::NRpcProxy::NProto::TTableReaderPayload& payload)
        : Underlying_ (std::move(underlying))
        , StartRowIndex_ (startRowIndex)
        , KeyColumns_(keyColumns)
        , OmittedInaccessibleColumns_(omittedInaccessibleColumns)
        , NameTable_ (New<TNameTable>())
        , ReadyEvent_ (NewPromise<void>())
    {
        YCHECK(Underlying_);
        RowsetDescriptor_.set_wire_format_version(1);
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
        auto guard = Guard(EventLock_);
        return ReadyEvent_;
    }

    virtual bool Read(std::vector<TUnversionedRow>* rows) override
    {
        YCHECK(rows->capacity() > 0);
        rows->clear();

        {
            auto guard = Guard(EventLock_);
            if (!ReadyEvent_.IsSet() || !ReadyEvent_.Get().IsOK()) {
                return true;
            }

            if (Finished_) {
                return false;
            }

            ReadyEvent_ = NewPromise<void>();
        }

        UserRows_.clear();
        while (AsyncRowsWithPayload_ && AsyncRowsWithPayload_.IsSet() && AsyncRowsWithPayload_.Get().IsOK()
            && !Finished_ && rows->size() < rows->capacity())
        {
            const auto& currentRows = AsyncRowsWithPayload_.Get().Value().Rows;
            const auto& currentPayload = AsyncRowsWithPayload_.Get().Value().Payload;

            if (currentRows.Empty()) {
                auto guard = Guard(EventLock_);
                ReadyEvent_.Set();
                Finished_ = true;
                ApplyReaderPayload(currentPayload);
                continue;
            }
            size_t readRowsCount = std::min(
                rows->capacity() - rows->size(),
                currentRows.Size() - CurrentRowsOffset_);
            RowCount_ += readRowsCount;
            size_t newOffset = CurrentRowsOffset_ + readRowsCount;
            for (size_t rowIndex = CurrentRowsOffset_; rowIndex < newOffset; ++rowIndex) {
                rows->push_back(currentRows[rowIndex]);
                DataWeight_ += GetDataWeight(currentRows[rowIndex]);
            }
            CurrentRowsOffset_ = newOffset;

            UserRows_.push_back(currentRows);
            ApplyReaderPayload(currentPayload);

            if (CurrentRowsOffset_ == currentRows.size()) {
                AsyncRowsWithPayload_ = GetRowsWithPayload();
                CurrentRowsOffset_ = 0;
            }
        }

        {
            auto guard = Guard(EventLock_);
            ReadyEvent_ .TrySetFrom(AsyncRowsWithPayload_);
        }

        return true;
    }

    virtual const TNameTablePtr& GetNameTable() const override
    {
        return NameTable_;
    }

    virtual const TKeyColumns& GetKeyColumns() const override
    {
        return KeyColumns_;
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
    const std::vector<TString> OmittedInaccessibleColumns_;

    NChunkClient::NProto::TDataStatistics DataStatistics_;
    i64 TotalRowCount_;

    size_t RowCount_ = 0;
    size_t DataWeight_ = 0;

    TNameTablePtr NameTable_;
    TNameTableToSchemaIdMapping IdMapping_;
    NApi::NRpcProxy::NProto::TRowsetDescriptor RowsetDescriptor_;

    TSpinLock EventLock_;
    TPromise<void> ReadyEvent_;

    std::vector<TRows> UserRows_;
    TFuture<TRowsWithPayload> AsyncRowsWithPayload_;
    size_t CurrentRowsOffset_ = 0;

    bool Finished_ = false;

    void ApplyReaderPayload(const TPayload& payload)
    {
        TotalRowCount_ = payload.total_row_count();
        DataStatistics_ = payload.data_statistics();
    }

    TFuture<TRowsWithPayload> GetRowsWithPayload()
    {
        return Underlying_->Read()
            .Apply(BIND([weakThis = MakeWeak(this)] (const TSharedRef& rowData) {
                auto this_ = weakThis.Lock();
                if (!this_) {
                    THROW_ERROR_EXCEPTION("Reader abandoned");
                }

                auto rowsWithPayload = this_->DeserializeRows(rowData);
                if (rowsWithPayload.Rows.Empty()) {
                    return ExpectEndOfStream(this_->Underlying_).Apply(BIND([=] () {
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
            THROW_ERROR_EXCEPTION("Error deserializing rows in table reader: expected %v packed refs, got %v",
                2,
                parts.size());
        }

        auto rowsetData = parts[0];
        auto payloadRef = parts[1];

        auto rows = DeserializeRowsetWithNameTableDelta(
            rowsetData,
            NameTable_,
            &RowsetDescriptor_,
            &IdMapping_);

        TPayload payload;
        if (!TryDeserializeProto(&payload, payloadRef)) {
            THROW_ERROR_EXCEPTION("Error deserializing table reader payload");
        }

        return { std::move(rows), std::move(payload) };
    }
};

TFuture<ITableReaderPtr> CreateRpcProxyTableReader(
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

                return New<TTableReader>(
                    inputStream,
                    startRowIndex,
                    std::move(keyColumns),
                    std::move(omittedInaccessibleColumns),
                    meta.payload());
            })).As<ITableReaderPtr>();
        }));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NRpcProxy

