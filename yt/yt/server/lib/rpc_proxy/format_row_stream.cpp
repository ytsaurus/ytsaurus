#include "format_row_stream.h"

#include <yt/yt/client/api/table_writer.h>
#include <yt/yt/client/api/rpc_proxy/helpers.h>
#include <yt/yt/client/api/rpc_proxy/row_stream.h>

#include <yt/yt/client/table_client/adapters.h>
#include <yt/yt/client/table_client/unversioned_row.h>
#include <yt/yt/client/table_client/row_batch.h>
#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/row_buffer.h>
#include <yt/yt/client/table_client/table_output.h>
#include <yt/yt/client/table_client/value_consumer.h>

#include <yt/yt/core/misc/range.h>
#include <yt/yt/core/misc/protobuf_helpers.h>

#include <yt/yt/library/formats/format.h>

namespace NYT::NApi::NRpcProxy {

using namespace NConcurrency;
using namespace NFormats;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

class TFormatStreamEncoder
    : public IRowStreamEncoder
{
public:
    TFormatStreamEncoder(
        TNameTablePtr nameTable,
        TFormat format,
        TTableSchemaPtr tableSchema,
        TControlAttributesConfigPtr controlAttributesConfig)
        : NameTable_(std::move(nameTable))
        , OutputStream_(Data_)
        , AsyncOutputStream_(CreateAsyncAdapter(&OutputStream_))
        , Writer_(CreateStaticTableWriterForFormat(
            format,
            NameTable_,
            {tableSchema},
            AsyncOutputStream_,
            /*enableContextSaving*/ false,
            controlAttributesConfig,
            /*keyColumnCount*/ 0))
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

        Data_.clear();
        Writer_->WriteBatch(batch);
        WaitFor(Writer_->Flush())
            .ThrowOnError();

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
    const TFormat Format_;
    const TTableSchemaPtr TableSchema_;
    const TControlAttributesConfigPtr ControlAttributesConfig_;

    TString Data_;
    TStringOutput OutputStream_;
    IFlushableAsyncOutputStreamPtr AsyncOutputStream_;

    const ISchemalessFormatWriterPtr Writer_;

    int NameTableSize_ = 0;
};

IRowStreamEncoderPtr CreateFormatRowStreamEncoder(
    TNameTablePtr nameTable,
    TFormat format,
    TTableSchemaPtr tableSchema,
    TControlAttributesConfigPtr controlAttributesConfig)
{
    return New<TFormatStreamEncoder>(
        std::move(nameTable),
        std::move(format),
        std::move(tableSchema),
        std::move(controlAttributesConfig));
}

////////////////////////////////////////////////////////////////////////////////

struct TCapturingTableWriterBufferTag
{ };

class TCapturingTableWriter
    : public ITableWriter
{
public:
    TCapturingTableWriter(TNameTablePtr nameTable, NTableClient::TTableSchemaPtr tableSchema)
        : NameTable_(std::move(nameTable))
        , TableSchema_(std::move(tableSchema))
        , RowBuffer_(New<TRowBuffer>(TCapturingTableWriterBufferTag()))
    { }

    bool Write(TRange<NTableClient::TUnversionedRow> rows) override
    {
        auto capturedRows = RowBuffer_->CaptureRows(rows);
        std::move(capturedRows.begin(), capturedRows.end(), std::back_inserter(WrittenRows_));
        return true;
    }

    TFuture<void> GetReadyEvent() override
    {
        return VoidFuture;
    }

    TFuture<void> Close() override
    {
        return VoidFuture;
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
        WrittenRows_.clear();
        RowBuffer_->Clear();
    }

    const std::vector<NTableClient::TUnversionedRow>& GetWrittenRows()
    {
        return WrittenRows_;
    }

private:
    const TNameTablePtr NameTable_;
    const NTableClient::TTableSchemaPtr TableSchema_;
    const TRowBufferPtr RowBuffer_;

    std::vector<NTableClient::TUnversionedRow> WrittenRows_;
};

class TFormatStreamDecoder
    : public IRowStreamDecoder
{
public:
    TFormatStreamDecoder(TNameTablePtr nameTable, TFormat format, NTableClient::TTableSchemaPtr tableSchema)
        : NameTable_(std::move(nameTable))
        , Format_(std::move(format))
        , TableSchema_(std::move(tableSchema))
        , TableWriter_(New<TCapturingTableWriter>(NameTable_, TableSchema_))
        , SchemalessWriter_(CreateSchemalessFromApiWriterAdapter(TableWriter_))
        , ValueConsumer_(
            SchemalessWriter_,
            ConvertTo<TTypeConversionConfigPtr>(format.Attributes()))
        , Output_(CreateParserForFormat(Format_, &ValueConsumer_))
    {
    }

    IUnversionedRowBatchPtr Decode(
        const TSharedRef& payloadRef,
        const NProto::TRowsetDescriptor& /*descriptorDelta*/) override
    {
        TableWriter_->Clear();
        Output_.Write(payloadRef.Begin(), payloadRef.Size());
        WaitFor(ValueConsumer_.Flush())
            .ThrowOnError();

        auto rows = MakeSharedRange(TableWriter_->GetWrittenRows());

        return CreateBatchFromUnversionedRows(std::move(rows));
    }

private:
    const TNameTablePtr NameTable_;
    const TFormat Format_;
    const NTableClient::TTableSchemaPtr TableSchema_;
    const TIntrusivePtr<TCapturingTableWriter> TableWriter_;

    IUnversionedWriterPtr SchemalessWriter_;
    TWritingValueConsumer ValueConsumer_;
    TTableOutput Output_;
};

IRowStreamDecoderPtr CreateFormatRowStreamDecoder(TNameTablePtr nameTable, TFormat format, NTableClient::TTableSchemaPtr tableSchema)
{
    return New<TFormatStreamDecoder>(std::move(nameTable), std::move(format), std::move(tableSchema));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NRpcProxy
