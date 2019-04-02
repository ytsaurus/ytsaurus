#include "table_writer.h"

#include "helpers.h"

#include <yt/client/table_client/name_table.h>
#include <yt/client/table_client/schema.h>

#include <yt/core/rpc/stream.h>

namespace NYT::NApi::NRpcProxy {

using namespace NConcurrency;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

class TRpcTableWriter
    : public ITableWriter
{
public:
    TRpcTableWriter(
        IAsyncZeroCopyOutputStreamPtr underlying,
        const TTableSchema& schema)
        : Underlying_ (std::move(underlying))
        , Schema_(schema)
        , NameTable_ (TNameTable::FromSchema(schema))
        , GetReadyEvent_ (MakePromise<void>(TError()))
    {
        YCHECK(Underlying_);
    }

    virtual bool Write(TRange<TUnversionedRow> rows) override {
        ValidateNotClosed();

        auto promise = NewPromise<void>();

        {
            auto guard = Guard(EventLock_);
            if (!GetReadyEvent_.IsSet() || !GetReadyEvent_.Get().IsOK()) {
                THROW_ERROR_EXCEPTION("TRpcTableWriter::Write() was called before waiting for GetReadyEvent()");
            }

            GetReadyEvent_ = promise;
        }

        auto rowData = SerializeRowsToRef(rows);
        Underlying_->Write(rowData).Subscribe(BIND ([=] (const TError& error) mutable {
            promise.Set(error);
        }));

        return promise.IsSet(); // TODO(kiselyovp) look at the streaming implementation to see if this could be too slow
    }

    virtual TFuture<void> GetReadyEvent() override {
        ValidateNotClosed();

        auto guard = Guard(EventLock_);
        return GetReadyEvent_;
    }

    virtual TFuture<void> Close() override {
        ValidateNotClosed();
        Closed_ = true;

        return Underlying_->Close();
    }

    virtual const TNameTablePtr& GetNameTable() const override {
        return NameTable_;
    }

    virtual const TTableSchema& GetSchema() const override {
        return Schema_;
    }

private:
    IAsyncZeroCopyOutputStreamPtr Underlying_;
    TTableSchema Schema_;
    TNameTablePtr NameTable_;

    TPromise<void> GetReadyEvent_;
    TSpinLock EventLock_;

    std::atomic<bool> Closed_ = {false};

    void ValidateNotClosed()
    {
        if (Closed_) {
            THROW_ERROR_EXCEPTION("Table writer is closed");
        }
    }

    TSharedRef SerializeRowsToRef(TRange<TUnversionedRow> rows) {
        NRpcProxy::NProto::TRowsetDescriptor descriptor;
        const auto& rowRefs = SerializeRowset(
            NameTable_, // TODO(kiselyovp) race or no race??
            rows,
            &descriptor);

        auto descriptorRef = SerializeProtoToRef(descriptor);
        struct TRpcFileWriterTag { };
        auto mergedRowRefs = MergeRefsToRef<TRpcFileWriterTag>(rowRefs);

        return PackRefs(std::vector { descriptorRef, mergedRowRefs });
    }
};

TFuture<ITableWriterPtr> CreateRpcTableWriter(
    TApiServiceProxy::TReqCreateTableWriterPtr request)
{
    auto schemaHolder = std::make_unique<TTableSchema>();
    auto createStreamResult = NRpc::CreateOutputStreamAdapter(
        request,
        BIND ([=, schema = schemaHolder.get()] (const TSharedRef& metaRef) {
            NApi::NRpcProxy::NProto::TMetaCreateTableWriter meta;
            if (!TryDeserializeProto(&meta, metaRef)) {
                THROW_ERROR_EXCEPTION("Failed to deserialize schema for table writer");
            }

            FromProto(schema, meta.schema());
        }));

    return createStreamResult.Apply(BIND([=, schemaHolder = std::move(schemaHolder)] (const IAsyncZeroCopyOutputStreamPtr& outputStream) {
            return New<TRpcTableWriter>(outputStream, *schemaHolder);
        })).As<ITableWriterPtr>();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NRpcProxy

