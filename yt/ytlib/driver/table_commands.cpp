#include "stdafx.h"
#include "table_commands.h"
#include "config.h"

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

#include <ytlib/new_table_client/schemaful_chunk_reader.h>
#include <ytlib/new_table_client/schemaful_chunk_writer.h>
#include <ytlib/new_table_client/config.h>
#include <ytlib/new_table_client/name_table.h>
#include <ytlib/new_table_client/unversioned_row.h>
#include <ytlib/new_table_client/schemaful_writer.h>

#include <ytlib/tablet_client/table_mount_cache.h>

#include <ytlib/query_client/plan_fragment.h>
#include <ytlib/query_client/executor.h>

#include <ytlib/hive/cell_directory.h>

#include <ytlib/transaction_client/transaction_manager.h>

#include <ytlib/new_table_client/name_table.h>
#include <ytlib/new_table_client/unversioned_row.h>

#include <ytlib/api/transaction.h>
#include <ytlib/api/rowset.h>

namespace NYT {
namespace NDriver {

using namespace NYson;
using namespace NYTree;
using namespace NFormats;
using namespace NChunkClient;
using namespace NTableClient;
using namespace NQueryClient;
using namespace NConcurrency;
using namespace NTransactionClient;
using namespace NHive;
using namespace NVersionedTableClient;
using namespace NApi;

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
        Context->GetClient()->GetMasterChannel(),
        GetTransaction(EAllowNullTransaction::Yes, EPingTransaction::Yes),
        Context->GetClient()->GetConnection()->GetBlockCache(),
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
        ProduceRow(consumer.get(), reader->GetRow());

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
    // COMPAT(babenko): remove Request->TableWriter
    auto config = UpdateYsonSerializable(
        Context->GetConfig()->TableWriter,
        Request->TableWriter);
    config = UpdateYsonSerializable(
        config,
        Request->GetOptions());

    auto writer = CreateAsyncTableWriter(
        config,
        Context->GetClient()->GetMasterChannel(),
        GetTransaction(EAllowNullTransaction::Yes, EPingTransaction::Yes),
        Context->GetClient()->GetTransactionManager(),
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

void TMountTableCommand::DoExecute()
{
    TMountTableOptions options;
    if (Request->FirstTabletIndex) {
        options.FirstTabletIndex = *Request->FirstTabletIndex;
    }
    if (Request->LastTabletIndex) {
        options.LastTabletIndex = *Request->LastTabletIndex;
    }

    auto result = WaitFor(Context->GetClient()->MountTable(
        Request->Path.GetPath(),
        options));
    THROW_ERROR_EXCEPTION_IF_FAILED(result);
}

////////////////////////////////////////////////////////////////////////////////

void TUnmountTableCommand::DoExecute()
{
    TUnmountTableOptions options;
    if (Request->FirstTabletIndex) {
        options.FirstTabletIndex = *Request->FirstTabletIndex;
    }
    if (Request->LastTabletIndex) {
        options.LastTabletIndex = *Request->LastTabletIndex;
    }
    options.Force = Request->Force;

    auto result = WaitFor(Context->GetClient()->UnmountTable(
        Request->Path.GetPath(),
        options));
    THROW_ERROR_EXCEPTION_IF_FAILED(result);
}

////////////////////////////////////////////////////////////////////////////////

void TReshardTableCommand::DoExecute()
{
    TReshardTableOptions options;
    if (Request->FirstTabletIndex) {
        options.FirstTabletIndex = *Request->FirstTabletIndex;
    }
    if (Request->LastTabletIndex) {
        options.LastTabletIndex = *Request->LastTabletIndex;
    }
    
    std::vector<TUnversionedRow> pivotKeys;
    for (const auto& key : Request->PivotKeys) {
        pivotKeys.push_back(key.Get());
    }
    
    auto result = WaitFor(Context->GetClient()->ReshardTable(
        Request->Path.GetPath(),
        pivotKeys,
        options));
    THROW_ERROR_EXCEPTION_IF_FAILED(result);
}

////////////////////////////////////////////////////////////////////////////////

void TInsertCommand::DoExecute()
{
    // COMPAT(babenko): remove Request->TableWriter
    auto config = UpdateYsonSerializable(
        Context->GetConfig()->TableWriter,
        Request->TableWriter);
    config = UpdateYsonSerializable(
        config,
        Request->GetOptions());

    auto tableMountCache = Context->GetClient()->GetConnection()->GetTableMountCache();
    auto tableInfoOrError = WaitFor(tableMountCache->GetTableInfo(Request->Path.GetPath()));
    THROW_ERROR_EXCEPTION_IF_FAILED(tableInfoOrError);
    const auto& tableInfo = tableInfoOrError.Value();

    // Parse input data.
    TBuildingTableConsumer consumer(
        tableInfo->Schema,
        tableInfo->KeyColumns);
    consumer.SetTreatMissingAsNull(!Request->Update);
    consumer.SetAllowNonSchemaColumns(false);

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

    // Write data into the tablets.

    NApi::TTransactionStartOptions startOptions;
    startOptions.Type = ETransactionType::Tablet;
    auto transactionOrError = WaitFor(Context->GetClient()->StartTransaction(startOptions));
    THROW_ERROR_EXCEPTION_IF_FAILED(transactionOrError);
    auto transaction = transactionOrError.Value();

    // Convert to non-owning.
    std::vector<TUnversionedRow> rows;
    for (const auto& row : consumer.Rows()) {
        rows.emplace_back(row.Get());
    }

    transaction->WriteRows(
        Request->Path.GetPath(),
        consumer.GetNameTable(),
        std::move(rows));

    auto commitResult = WaitFor(transaction->Commit());
    THROW_ERROR_EXCEPTION_IF_FAILED(commitResult);
}

////////////////////////////////////////////////////////////////////////////////

void TSelectCommand::DoExecute()
{
    // TODO(babenko): read output via streaming
    TSelectRowsOptions options;
    options.Timestamp = Request->Timestamp;

    auto rowsetOrError = WaitFor(Context->GetClient()->SelectRows(
        Request->Query,
        options));
    THROW_ERROR_EXCEPTION_IF_FAILED(rowsetOrError);

    auto rowset = rowsetOrError.Value();
    auto nameTable = rowset->GetNameTable();

    TBlobOutput buffer;
    auto format = Context->GetOutputFormat();
    auto consumer = CreateConsumerForFormat(format, EDataType::Tabular, &buffer);

    for (auto row : rowset->GetRows()) {
        consumer->OnListItem();
        consumer->OnBeginMap();
        for (int i = 0; i < row.GetCount(); ++i) {
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

    auto output = Context->Request().OutputStream;
    if (!output->Write(buffer.Begin(), buffer.Size())) {
        auto result = WaitFor(output->GetReadyEvent());
        THROW_ERROR_EXCEPTION_IF_FAILED(result);
    }
    buffer.Clear();
}

////////////////////////////////////////////////////////////////////////////////

void TLookupCommand::DoExecute()
{
    auto tableMountCache = Context->GetClient()->GetConnection()->GetTableMountCache();
    auto tableInfoOrError = WaitFor(tableMountCache->GetTableInfo(Request->Path.GetPath()));
    THROW_ERROR_EXCEPTION_IF_FAILED(tableInfoOrError);
    const auto& tableInfo = tableInfoOrError.Value();
    auto nameTable = TNameTable::FromSchema(tableInfo->Schema);

    TLookupRowsOptions options;
    options.Timestamp = Request->Timestamp;
    options.ColumnNames = Request->ColumnNames;

    auto lookupResult = WaitFor(Context->GetClient()->LookupRow(
        Request->Path.GetPath(),
        Request->Key.Get(),
        options));
    THROW_ERROR_EXCEPTION_IF_FAILED(lookupResult);
    
    auto rowset = lookupResult.Value();
    YCHECK(rowset->GetRows().size() == 1);
    auto row = rowset->GetRows()[0];
    if (row) {
        TBlobOutput buffer;
        auto format = Context->GetOutputFormat();
        auto consumer = CreateConsumerForFormat(format, EDataType::Tabular, &buffer);
        
        consumer->OnListItem();
        consumer->OnBeginMap();
        for (int index = 0; index < row.GetCount(); ++index) {
            const auto& value = row[index];
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

        auto output = Context->Request().OutputStream;
        if (!output->Write(buffer.Begin(), buffer.Size())) {
            auto result = WaitFor(output->GetReadyEvent());
            THROW_ERROR_EXCEPTION_IF_FAILED(result);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

void TDeleteCommand::DoExecute()
{
    NTransactionClient::TTransactionStartOptions startOptions;
    startOptions.Type = ETransactionType::Tablet;
    auto transactionOrError = WaitFor(Context->GetClient()->StartTransaction(startOptions));
    THROW_ERROR_EXCEPTION_IF_FAILED(transactionOrError);
    auto transaction = transactionOrError.Value();

    transaction->DeleteRow(Request->Path.GetPath(), Request->Key.Get());

    auto commitResult = WaitFor(transaction->Commit());
    THROW_ERROR_EXCEPTION_IF_FAILED(commitResult);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT
