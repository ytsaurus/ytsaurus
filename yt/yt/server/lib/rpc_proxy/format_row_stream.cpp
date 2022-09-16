#include "format_row_stream.h"

#include <yt/yt/client/api/table_writer.h>
#include <yt/yt/client/api/rpc_proxy/helpers.h>
#include <yt/yt/client/api/rpc_proxy/row_stream.h>

#include <yt/yt/client/formats/format.h>

#include <yt/yt/client/table_client/adapters.h>
#include <yt/yt/client/table_client/unversioned_row.h>
#include <yt/yt/client/table_client/row_batch.h>
#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/row_buffer.h>
#include <yt/yt/client/table_client/table_output.h>
#include <yt/yt/client/table_client/value_consumer.h>

#include <yt/yt/core/misc/range.h>
#include <yt/yt/core/misc/protobuf_helpers.h>

namespace NYT::NApi::NRpcProxy {

using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

class TFormatStreamEncoder
    : public IRowStreamEncoder
{
public:
    explicit TFormatStreamEncoder(
        TNameTablePtr nameTable,
        NFormats::TFormat format,
        TTableSchemaPtr tableSchema,
        NFormats::TControlAttributesConfigPtr controlAttributesConfig)
        : NameTable_(std::move(nameTable))
        , OutputStream_(Data_)
        , AsyncOutputStream_(NConcurrency::CreateAsyncAdapter(&OutputStream_))
        , Writer_(NFormats::CreateStaticTableWriterForFormat(
            format,
            NameTable_,
            {tableSchema},
            AsyncOutputStream_,
            false,
            controlAttributesConfig,
            0))
    { }

    TSharedRef Encode(
        const IUnversionedRowBatchPtr& batch,
        const NApi::NRpcProxy::NProto::TRowsetStatistics* statistics) override
    {
        YT_VERIFY(NameTableSize_ <= NameTable_->GetSize());

        NProto::TRowsetDescriptor descriptor;
        descriptor.set_wire_format_version(NApi::NRpcProxy::CurrentWireFormatVersion);
        descriptor.set_rowset_kind(NApi::NRpcProxy::NProto::RK_UNVERSIONED);
        descriptor.set_rowset_format(NApi::NRpcProxy::NProto::RF_FORMAT);
        for (int id = NameTableSize_; id < NameTable_->GetSize(); ++id) {
            auto* entry = descriptor.add_name_table_entries();
            entry->set_name(TString(NameTable_->GetName(id)));
        }
        NameTableSize_ += descriptor.name_table_entries_size();

        auto rows = batch->MaterializeRows();

        Data_.clear();
        Writer_->Write(rows);
        Writer_->Flush();

        auto rowRefs = TSharedRef::FromString(Data_);

        auto [block, payloadRef] = SerializeRowStreamBlockEnvelope(
            rowRefs.Size(),
            descriptor,
            statistics);

        MergeRefsToRef(std::vector<TSharedRef>{rowRefs}, payloadRef);

        return block;
    }

private:
    const TNameTablePtr NameTable_;
    const NFormats::TFormat Format_;
    const TTableSchemaPtr TableSchema_;
    const NFormats::TControlAttributesConfigPtr ControlAttributesConfig_;

    TString Data_;
    TStringOutput OutputStream_;
    NConcurrency::IFlushableAsyncOutputStreamPtr AsyncOutputStream_;

    NFormats::ISchemalessFormatWriterPtr Writer_;

    int NameTableSize_ = 0;
};

IRowStreamEncoderPtr CreateFormatRowStreamEncoder(
        TNameTablePtr nameTable,
        NFormats::TFormat format,
        TTableSchemaPtr tableSchema,
        NFormats::TControlAttributesConfigPtr controlAttributesConfig)
{
    return New<TFormatStreamEncoder>(
        std::move(nameTable), std::move(format), std::move(tableSchema), std::move(controlAttributesConfig));
}

////////////////////////////////////////////////////////////////////////////////

class TStorageTableWriter : public ITableWriter
{
public:
    TStorageTableWriter(TNameTablePtr nameTable, NTableClient::TTableSchemaPtr tableSchema)
        : NameTable_(std::move(nameTable))
        , TableSchema_(std::move(tableSchema))
    { }

    bool Write(TRange<NTableClient::TUnversionedRow> rows) override
    {
        std::move(rows.begin(), rows.end(), std::back_inserter(WrittenRanges_));
        return true;
    }

    TFuture<void> GetReadyEvent() override
    {
        return MakeFuture<void>({});
    }

    TFuture<void> Close() override
    {
        return MakeFuture<void>({});
    }

    const NTableClient::TNameTablePtr& GetNameTable() const override
    {
        return NameTable_;
    }

    const NTableClient::TTableSchemaPtr& GetSchema() const override
    {
        return TableSchema_;
    }

    void Clear()
    {
        WrittenRanges_.clear();
    }

    const std::vector<NTableClient::TUnversionedRow>& GetWrittenRows()
    {
        return WrittenRanges_;
    }

private:
    const TNameTablePtr NameTable_;
    const NTableClient::TTableSchemaPtr TableSchema_;

    std::vector<NTableClient::TUnversionedRow> WrittenRanges_;
};

class TFormatStreamDecoder
    : public IRowStreamDecoder
{
public:
    explicit TFormatStreamDecoder(TNameTablePtr nameTable, NFormats::TFormat format, NTableClient::TTableSchemaPtr tableSchema)
        : NameTable_(std::move(nameTable))
        , Format_(std::move(format))
        , TableSchema_(std::move(tableSchema))
        , TableWriter_(New<TStorageTableWriter>(NameTable_, TableSchema_))
        , SchemalessWriter_(CreateSchemalessFromApiWriterAdapter(TableWriter_))
        , ValueConsumer_(
            SchemalessWriter_,
            ConvertTo<TTypeConversionConfigPtr>(format.Attributes()))
        , Output_(NFormats::CreateParserForFormat(Format_, &ValueConsumer_))
    {
    }

    IUnversionedRowBatchPtr Decode(
        const TSharedRef& payloadRef,
        const NProto::TRowsetDescriptor& descriptorDelta) override
    {
        Y_UNUSED(descriptorDelta);

        TableWriter_->Clear();
        Output_.Write(payloadRef.Begin(), payloadRef.Size());
        NConcurrency::WaitFor(ValueConsumer_.Flush()).ThrowOnError();

        auto rows = MakeSharedRange(TableWriter_->GetWrittenRows());

        return CreateBatchFromUnversionedRows(std::move(rows));
    }

private:
    const TNameTablePtr NameTable_;
    const NFormats::TFormat Format_;
    const NTableClient::TTableSchemaPtr TableSchema_;

    TIntrusivePtr<TStorageTableWriter> TableWriter_;
    IUnversionedWriterPtr SchemalessWriter_;
    TWritingValueConsumer ValueConsumer_;
    TTableOutput Output_;
};

IRowStreamDecoderPtr CreateFormatRowStreamDecoder(TNameTablePtr nameTable, NFormats::TFormat format, NTableClient::TTableSchemaPtr tableSchema)
{
    return New<TFormatStreamDecoder>(std::move(nameTable), std::move(format), std::move(tableSchema));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NRpcProxy
