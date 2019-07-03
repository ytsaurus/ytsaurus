#include "table_writer.h"
#include "helpers.h"

#include <yt/client/api/table_writer.h>

#include <yt/client/table_client/name_table.h>
#include <yt/client/table_client/schema.h>

#include <yt/core/rpc/stream.h>

namespace NYT::NApi::NRpcProxy {

using namespace NConcurrency;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

class TTableWriter
    : public ITableWriter
{
public:
    TTableWriter(
        IAsyncZeroCopyOutputStreamPtr underlying,
        const TTableSchema& schema)
        : Underlying_(std::move(underlying))
        , Schema_(schema)
    {
        YT_VERIFY(Underlying_);
        NameTable_->SetEnableColumnNameValidation();
    }

    virtual bool Write(TRange<TUnversionedRow> rows) override
    {
        ValidateNotClosed();

        if (!ReadyEvent_.IsSet() || !ReadyEvent_.Get().IsOK()) {
            THROW_ERROR_EXCEPTION("Write() was called before waiting for GetReadyEvent()");
        }

        ReadyEvent_ = NewPromise<void>();

        auto rowData = SerializeRowsetWithNameTableDelta(
            NameTable_,
            rows,
            &NameTableSize_);
        ReadyEvent_.TrySetFrom(Underlying_->Write(rowData));

        return ReadyEvent_.IsSet() && ReadyEvent_.Get().IsOK();
    }

    virtual TFuture<void> GetReadyEvent() override
    {
        ValidateNotClosed();
        return ReadyEvent_;
    }

    virtual TFuture<void> Close() override
    {
        ValidateNotClosed();
        Closed_ = true;

        return Underlying_->Close();
    }

    virtual const TNameTablePtr& GetNameTable() const override
    {
        return NameTable_;
    }

    virtual const TTableSchema& GetSchema() const override
    {
        return Schema_;
    }

private:
    const IAsyncZeroCopyOutputStreamPtr Underlying_;
    const TTableSchema Schema_;
    const TNameTablePtr NameTable_ = New<TNameTable>();

    int NameTableSize_ = 0;

    TPromise<void> ReadyEvent_ = MakePromise<void>(TError());

    bool Closed_ = false;

    void ValidateNotClosed()
    {
        if (Closed_) {
            THROW_ERROR_EXCEPTION("Table writer is closed");
        }
    }
};

TFuture<ITableWriterPtr> CreateTableWriter(
    TApiServiceProxy::TReqWriteTablePtr request)
{
    auto schemaHolder = std::make_unique<TTableSchema>();
    auto futureStream = NRpc::CreateRpcClientOutputStream(
        std::move(request),
        BIND ([schema = schemaHolder.get()] (const TSharedRef& metaRef) {
            NApi::NRpcProxy::NProto::TWriteTableMeta meta;
            if (!TryDeserializeProto(&meta, metaRef)) {
                THROW_ERROR_EXCEPTION("Failed to deserialize schema for table writer");
            }

            FromProto(schema, meta.schema());
        }));

    return futureStream.Apply(
        BIND([schemaHolder = std::move(schemaHolder)] (const IAsyncZeroCopyOutputStreamPtr& outputStream) {
            return New<TTableWriter>(outputStream, *schemaHolder);
        })).As<ITableWriterPtr>();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NRpcProxy
