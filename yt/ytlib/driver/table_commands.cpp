#include "stdafx.h"
#include "table_commands.h"
#include "config.h"
#include "table_mount_cache.h"

#include <core/concurrency/async_stream.h>
#include <core/concurrency/fiber.h>

#include <core/yson/parser.h>
#include <core/yson/consumer.h>

#include <core/ytree/fluent.h>

#include <ytlib/formats/parser.h>

#include <ytlib/transaction_client/transaction_manager.h>

#include <ytlib/chunk_client/block_cache.h>
#include <ytlib/chunk_client/memory_reader.h>
#include <ytlib/chunk_client/memory_writer.h>

#include <ytlib/table_client/table_reader.h>
#include <ytlib/table_client/table_writer.h>
#include <ytlib/table_client/table_consumer.h>
#include <ytlib/table_client/table_producer.h>

#include <ytlib/new_table_client/chunk_reader.h>
#include <ytlib/new_table_client/chunk_writer.h>
#include <ytlib/new_table_client/name_table.h>
#include <ytlib/new_table_client/reader.h>
#include <ytlib/new_table_client/row.h>

#include <ytlib/query_client/plan_fragment.h>
#include <ytlib/query_client/executor.h>

#include <ytlib/tablet_client/public.h>
#include <ytlib/tablet_client/tablet_service_proxy.h>

#include <ytlib/hive/cell_directory.h>

#include <ytlib/transaction_client/transaction.h>
#include <ytlib/transaction_client/transaction_manager.h>

#include <ytlib/new_table_client/chunk_writer.h>
#include <ytlib/new_table_client/name_table.h>
#include <ytlib/new_table_client/row.h>

#include <ytlib/tablet_client/tablet_service_proxy.h>

namespace NYT {
namespace NDriver {

using namespace NYson;
using namespace NYTree;
using namespace NFormats;
using namespace NChunkClient;
using namespace NTableClient;
using namespace NTabletClient;
using namespace NQueryClient;
using namespace NConcurrency;
using namespace NTransactionClient;
using namespace NHive;
using namespace NVersionedTableClient;

////////////////////////////////////////////////////////////////////////////////

namespace {

Stroka SerializeKey(const std::vector<INodePtr> key)
{
    TUnversionedRowBuilder keyBuilder;
    for (auto keyPart : key) {
        switch (keyPart->GetType()) {
            case ENodeType::Integer:
                keyBuilder.AddValue(MakeIntegerValue<TUnversionedValue>(keyPart->GetValue<i64>()));
                break;
            case ENodeType::Double:
                keyBuilder.AddValue(MakeDoubleValue<TUnversionedValue>(keyPart->GetValue<double>()));
                break;
            case ENodeType::String:
                // NB: keyPart will hold the value.
                keyBuilder.AddValue(MakeStringValue<TUnversionedValue>(keyPart->GetValue<Stroka>()));
                break;
            default:
                keyBuilder.AddValue(MakeAnyValue<TUnversionedValue>(ConvertToYsonString(keyPart).Data()));
                break;
        }
    }
    return ToProto<Stroka>(keyBuilder.GetRow());
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

void TReadCommand::DoExecute()
{
    // COMPAT(babenko): remove Request->TableReader
    auto config = UpdateYsonSerializable(
        Context->GetConfig()->TableReader,
        Request->TableReader);
    config = UpdateYsonSerializable(
        config,
        Request->GetOptions());

    auto reader = New<TAsyncTableReader>(
        config,
        Context->GetMasterChannel(),
        GetTransaction(EAllowNullTransaction::Yes, EPingTransaction::Yes),
        Context->GetBlockCache(),
        Request->Path);

    auto output = Context->Request().OutputStream;

    // TODO(babenko): provide custom allocation tag
    TBlobOutput buffer;
    i64 bufferLimit = Context->GetConfig()->ReadBufferSize;

    auto format = Context->GetOutputFormat();
    auto consumer = CreateConsumerForFormat(format, EDataType::Tabular, &buffer);

    reader->Open();

    auto fetchNextItem = [&] () -> bool {
        if (!reader->FetchNextItem()) {
            auto result = WaitFor(reader->GetReadyEvent());
            THROW_ERROR_EXCEPTION_IF_FAILED(result);
        }
        return reader->IsValid();
    };


    if (!fetchNextItem()) {
        return;
    }

    BuildYsonMapFluently(Context->Request().ResponseParametersConsumer)
        .Item("start_row_index").Value(reader->GetTableRowIndex());

    auto flushBuffer = [&] () {
        if (!output->Write(buffer.Begin(), buffer.Size())) {
            auto result = WaitFor(output->GetReadyEvent());
            THROW_ERROR_EXCEPTION_IF_FAILED(result);
        }
        buffer.Clear();
    };

    while (true) {
        ProduceRow(~consumer, reader->GetRow());

        if (buffer.Size() > bufferLimit) {
            flushBuffer();
        }

        if (!fetchNextItem()) {
            break;
        }
    }

    if (buffer.Size() > 0) {
        flushBuffer();
    }
}

//////////////////////////////////////////////////////////////////////////////////

void TWriteCommand::DoExecute()
{
    // COMPAT(babenko): remove Request->TableReader
    auto config = UpdateYsonSerializable(
        Context->GetConfig()->TableWriter,
        Request->TableWriter);
    config = UpdateYsonSerializable(
        config,
        Request->GetOptions());

    auto writer = CreateAsyncTableWriter(
        config,
        Context->GetMasterChannel(),
        GetTransaction(EAllowNullTransaction::Yes, EPingTransaction::Yes),
        Context->GetTransactionManager(),
        Request->Path,
        Request->Path.Attributes().Find<TKeyColumns>("sorted_by"));

    writer->Open();

    TTableConsumer consumer(writer);

    auto format = Context->GetInputFormat();
    auto parser = CreateParserForFormat(format, EDataType::Tabular, &consumer);

    struct TWriteBufferTag { };
    auto buffer = TSharedRef::Allocate<TWriteBufferTag>(config->BlockSize);

    auto input = Context->Request().InputStream;

    while (true) {
        if (!input->Read(buffer.Begin(), buffer.Size())) {
            auto result = WaitFor(input->GetReadyEvent());
            THROW_ERROR_EXCEPTION_IF_FAILED(result);
        }

        size_t length = input->GetReadLength();
        if (length == 0)
            break;

        parser->Read(TStringBuf(buffer.Begin(), length));

        if (!writer->IsReady()) {
            auto result = WaitFor(writer->GetReadyEvent());
            THROW_ERROR_EXCEPTION_IF_FAILED(result);
        }
    }

    parser->Finish();

    writer->Close();
}

////////////////////////////////////////////////////////////////////////////////

void TMountCommand::DoExecute()
{
    auto req = TTableYPathProxy::Mount(Request->Path.GetPath());

    auto rsp = WaitFor(ObjectProxy->Execute(req));
    THROW_ERROR_EXCEPTION_IF_FAILED(*rsp);

    ReplySuccess();
}

////////////////////////////////////////////////////////////////////////////////

void TUnmountCommand::DoExecute()
{
    auto req = TTableYPathProxy::Unmount(Request->Path.GetPath());

    auto rsp = WaitFor(ObjectProxy->Execute(req));
    THROW_ERROR_EXCEPTION_IF_FAILED(*rsp);

    ReplySuccess();
}

////////////////////////////////////////////////////////////////////////////////

void TInsertCommand::DoExecute()
{
    auto tableMountCache = Context->GetTableMountCache();
    auto mountInfoOrError = WaitFor(tableMountCache->LookupInfo(Request->Path.GetPath()));
    THROW_ERROR_EXCEPTION_IF_FAILED(mountInfoOrError);

    const auto& mountInfo = mountInfoOrError.GetValue();
    if (mountInfo->TabletId == NullTabletId) {
        THROW_ERROR_EXCEPTION("Table is not mounted");
    }

    auto config = UpdateYsonSerializable(
        Context->GetConfig()->NewTableWriter,
        Request->TableWriter);

    // Parse input data.

    auto nameTable = New<TNameTable>();

    auto memoryWriter = New<TMemoryWriter>();

    // TODO(babenko): make configurable
    auto encodingOptions = New<TEncodingWriterOptions>();

    auto chunkWriter = New<TChunkWriter>(
        config,
        encodingOptions,
        memoryWriter);

    TVersionedTableConsumer consumer(
        mountInfo->Schema,
        mountInfo->KeyColumns,
        nameTable,
        chunkWriter);

    chunkWriter->Open(
        nameTable,
        mountInfo->Schema,
        mountInfo->KeyColumns);

    auto format = Context->GetInputFormat();
    auto parser = CreateParserForFormat(format, EDataType::Tabular, &consumer);

    struct TWriteBufferTag { };
    auto buffer = TSharedRef::Allocate<TWriteBufferTag>(config->BlockSize);

    auto input = Context->Request().InputStream;

    while (true) {
        if (!input->Read(buffer.Begin(), buffer.Size())) {
            auto result = WaitFor(input->GetReadyEvent());
            THROW_ERROR_EXCEPTION_IF_FAILED(result);
        }

        size_t length = input->GetReadLength();
        if (length == 0)
            break;

        parser->Read(TStringBuf(buffer.Begin(), length));
    }

    parser->Finish();

    auto closeResult = WaitFor(chunkWriter->AsyncClose());
    THROW_ERROR_EXCEPTION_IF_FAILED(closeResult);

    // Write data into the tablet.

    auto transactionManager = Context->GetTransactionManager();
    TTransactionStartOptions startOptions;
    startOptions.Type = ETransactionType::Tablet;
    auto transactionOrError = WaitFor(transactionManager->AsyncStart(startOptions));
    THROW_ERROR_EXCEPTION_IF_FAILED(transactionOrError);
    auto transaction = transactionOrError.GetValue();

    transaction->AddParticipant(mountInfo->CellId);

    auto cellDirectory = Context->GetCellDirectory();
    auto channel = cellDirectory->GetChannelOrThrow(mountInfo->CellId);

    TTabletServiceProxy tabletProxy(channel);
    auto writeReq = tabletProxy.Write();
    ToProto(writeReq->mutable_transaction_id(), transaction->GetId());
    ToProto(writeReq->mutable_tablet_id(), mountInfo->TabletId);
    writeReq->mutable_chunk_meta()->Swap(&memoryWriter->GetChunkMeta());
    writeReq->Attachments() = std::move(memoryWriter->GetBlocks());

    auto writeRsp = WaitFor(writeReq->Invoke());
    THROW_ERROR_EXCEPTION_IF_FAILED(*writeRsp);

    auto commitResult = WaitFor(transaction->AsyncCommit());
    THROW_ERROR_EXCEPTION_IF_FAILED(commitResult);
}

////////////////////////////////////////////////////////////////////////////////

void TSelectCommand::DoExecute()
{
    using namespace NChunkClient;
    using namespace NVersionedTableClient;

    auto fragment = TPlanFragment::Prepare(
        Request->Query,
        Context->GetQueryCallbacksProvider()->GetPrepareCallbacks());

    auto coordinator = CreateCoordinatedEvaluator(
        GetCurrentInvoker(),
        Context->GetQueryCallbacksProvider()->GetCoordinateCallbacks());

    auto memoryWriter = New<TMemoryWriter>();
    auto chunkWriter = New<TChunkWriter>(
        New<NVersionedTableClient::TChunkWriterConfig>(),
        New<TEncodingWriterOptions>(),
        memoryWriter);

    {
        auto error = WaitFor(coordinator->Execute(fragment, chunkWriter));
        THROW_ERROR_EXCEPTION_IF_FAILED(error);
    }

    auto memoryReader = New<TMemoryReader>(
        std::move(memoryWriter->GetChunkMeta()),
        std::move(memoryWriter->GetBlocks()));
    auto chunkReader = CreateChunkReader(
        New<TChunkReaderConfig>(),
        memoryReader);

    TBlobOutput buffer;
    auto format = Context->GetOutputFormat();
    auto consumer = CreateConsumerForFormat(format, EDataType::Tabular, &buffer);

    auto nameTable = New<TNameTable>();
    {
        auto error = WaitFor(chunkReader->Open(
            nameTable,
            TTableSchema(),
            true));
        THROW_ERROR_EXCEPTION_IF_FAILED(error);
    }

    const int RowsBufferSize = 1000;
    std::vector<NVersionedTableClient::TVersionedRow> rows;
    rows.reserve(RowsBufferSize);
    
    while (true) {
        bool hasData = chunkReader->Read(&rows);
        for (auto row : rows) {
            consumer->OnListItem();
            consumer->OnBeginMap();
            for (int i = 0; i < row.GetValueCount(); ++i) {
                const auto& value = row[i];
                if (value.Type == EValueType::Null)
                    continue;
                consumer->OnKeyedItem(nameTable->GetName(value.Id));
                switch (value.Type) {
                    case EValueType::Integer:
                        consumer->OnIntegerScalar(value.Data.Integer);
                        break;
                    case EValueType::Double:
                        consumer->OnDoubleScalar(value.Data.Double);
                        break;
                    case EValueType::String:
                        consumer->OnStringScalar(TStringBuf(value.Data.String, value.Length));
                        break;
                    case EValueType::Any:
                        consumer->OnRaw(TStringBuf(value.Data.String, value.Length), EYsonType::Node);
                        break;
                    default:
                        YUNREACHABLE();
                }
            }
            consumer->OnEndMap();
        }
        if (!hasData) {
            break;
        }
        if (rows.size() < rows.capacity()) {
            auto result = WaitFor(chunkReader->GetReadyEvent());
            THROW_ERROR_EXCEPTION_IF_FAILED(result);
        }
        rows.clear();
    }

    auto output = Context->Request().OutputStream;
    if (!output->Write(buffer.Begin(), buffer.Size())) {
        auto result = WaitFor(output->GetReadyEvent());
        THROW_ERROR_EXCEPTION_IF_FAILED(result);
    }
    buffer.Clear();

    ReplySuccess();
}

////////////////////////////////////////////////////////////////////////////////

void TLookupCommand::DoExecute()
{
    auto tableMountCache = Context->GetTableMountCache();
    auto mountInfoOrError = WaitFor(tableMountCache->LookupInfo(Request->Path.GetPath()));
    THROW_ERROR_EXCEPTION_IF_FAILED(mountInfoOrError);

    const auto& mountInfo = mountInfoOrError.GetValue();
    if (mountInfo->TabletId == NullTabletId) {
        THROW_ERROR_EXCEPTION("Table is not mounted");
    }

    auto cellDirectory = Context->GetCellDirectory();
    auto channel = cellDirectory->GetChannelOrThrow(mountInfo->CellId);

    TTabletServiceProxy proxy(channel);
    auto req = proxy.Lookup();
    ToProto(req->mutable_tablet_id(), mountInfo->TabletId);
    req->set_timestamp(Request->Timestamp);
    if (Request->Columns) {
        req->set_all_columns(false);
        for (const auto& column : *Request->Columns) {
            req->add_columns(column);
        }
    }
    *req->mutable_key() = SerializeKey(Request->Key);

    auto rsp = WaitFor(req->Invoke());
    THROW_ERROR_EXCEPTION_IF_FAILED(*rsp);

    // TODO(babenko): eliminate copy-paste (see TSelectCommand::DoExecute).
    auto memoryReader = New<TMemoryReader>(
        std::move(*rsp->mutable_chunk_meta()),
        std::move(rsp->Attachments()));
    auto chunkReader = CreateChunkReader(
        New<TChunkReaderConfig>(),
        memoryReader);

    TBlobOutput buffer;
    auto format = Context->GetOutputFormat();
    auto consumer = CreateConsumerForFormat(format, EDataType::Tabular, &buffer);

    auto nameTable = New<TNameTable>();
    {
        auto error = WaitFor(chunkReader->Open(
            nameTable,
            TTableSchema(),
            true));
        THROW_ERROR_EXCEPTION_IF_FAILED(error);
    }

    const int RowsBufferSize = 1000;
    std::vector<NVersionedTableClient::TVersionedRow> rows;
    rows.reserve(RowsBufferSize);

    while (true) {
        bool hasData = chunkReader->Read(&rows);
        for (auto row : rows) {
            consumer->OnListItem();
            consumer->OnBeginMap();
            for (int i = 0; i < row.GetValueCount(); ++i) {
                const auto& value = row[i];
                if (value.Type == EValueType::Null)
                    continue;
                consumer->OnKeyedItem(nameTable->GetName(value.Id));
                switch (value.Type) {
                    case EValueType::Integer:
                        consumer->OnIntegerScalar(value.Data.Integer);
                        break;
                    case EValueType::Double:
                        consumer->OnDoubleScalar(value.Data.Double);
                        break;
                    case EValueType::String:
                        consumer->OnStringScalar(TStringBuf(value.Data.String, value.Length));
                        break;
                    case EValueType::Any:
                        consumer->OnRaw(TStringBuf(value.Data.String, value.Length), EYsonType::Node);
                        break;
                    default:
                        YUNREACHABLE();
                }
            }
            consumer->OnEndMap();
        }
        if (!hasData) {
            break;
        }
        if (rows.size() < rows.capacity()) {
            auto result = WaitFor(chunkReader->GetReadyEvent());
            THROW_ERROR_EXCEPTION_IF_FAILED(result);
        }
        rows.clear();
    }

    auto output = Context->Request().OutputStream;
    if (!output->Write(buffer.Begin(), buffer.Size())) {
        auto result = WaitFor(output->GetReadyEvent());
        THROW_ERROR_EXCEPTION_IF_FAILED(result);
    }
    buffer.Clear();

    ReplySuccess();
}

////////////////////////////////////////////////////////////////////////////////

void TDeleteCommand::DoExecute()
{
    auto tableMountCache = Context->GetTableMountCache();
    auto mountInfoOrError = WaitFor(tableMountCache->LookupInfo(Request->Path.GetPath()));
    THROW_ERROR_EXCEPTION_IF_FAILED(mountInfoOrError);

    const auto& mountInfo = mountInfoOrError.GetValue();
    if (mountInfo->TabletId == NullTabletId) {
        THROW_ERROR_EXCEPTION("Table is not mounted");
    }

    auto transactionManager = Context->GetTransactionManager();
    TTransactionStartOptions startOptions;
    startOptions.Type = ETransactionType::Tablet;
    auto transactionOrError = WaitFor(transactionManager->AsyncStart(startOptions));
    THROW_ERROR_EXCEPTION_IF_FAILED(transactionOrError);
    auto transaction = transactionOrError.GetValue();

    transaction->AddParticipant(mountInfo->CellId);

    auto cellDirectory = Context->GetCellDirectory();
    auto channel = cellDirectory->GetChannelOrThrow(mountInfo->CellId);

    TTabletServiceProxy proxy(channel);
    auto deleteReq = proxy.Delete();
    ToProto(deleteReq->mutable_transaction_id(), transaction->GetId());
    ToProto(deleteReq->mutable_tablet_id(), mountInfo->TabletId);
    *deleteReq->add_keys() = SerializeKey(Request->Key);

    auto deleteRsp = WaitFor(deleteReq->Invoke());
    THROW_ERROR_EXCEPTION_IF_FAILED(*deleteRsp);

    auto commitResult = WaitFor(transaction->AsyncCommit());
    THROW_ERROR_EXCEPTION_IF_FAILED(commitResult);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT
