#include "stdafx.h"
#include "table_commands.h"
#include "config.h"

#include <core/concurrency/async_stream.h>
#include <core/concurrency/scheduler.h>

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
#include <ytlib/new_table_client/table_producer.h>
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

static const auto& Logger = DriverLogger;

void TReadCommand::DoExecute()
{
    // COMPAT(babenko): remove Request_->TableReader
    auto config = UpdateYsonSerializable(
        Context_->GetConfig()->TableReader,
        Request_->TableReader);
    config = UpdateYsonSerializable(
        config,
        Request_->GetOptions());

    auto reader = New<TAsyncTableReader>(
        config,
        Context_->GetClient()->GetMasterChannel(),
        GetTransaction(EAllowNullTransaction::Yes, EPingTransaction::Yes),
        Context_->GetClient()->GetConnection()->GetBlockCache(),
        Request_->Path);

    auto output = Context_->Request().OutputStream;

    // TODO(babenko): provide custom allocation tag
    TBlobOutput buffer;
    i64 bufferLimit = Context_->GetConfig()->ReadBufferSize;

    auto format = Context_->GetOutputFormat();
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

    BuildYsonMapFluently(Context_->Request().ResponseParametersConsumer)
        .Item("start_row_index").Value(reader->GetTableRowIndex());

    auto flushBuffer = [&] () {
        auto result = WaitFor(output->Write(buffer.Begin(), buffer.Size()));
        THROW_ERROR_EXCEPTION_IF_FAILED(result);
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
    // COMPAT(babenko): remove Request_->TableWriter
    auto config = UpdateYsonSerializable(
        Context_->GetConfig()->TableWriter,
        Request_->TableWriter);
    config = UpdateYsonSerializable(
        config,
        Request_->GetOptions());

    auto writer = CreateAsyncTableWriter(
        config,
        Context_->GetClient()->GetMasterChannel(),
        GetTransaction(EAllowNullTransaction::Yes, EPingTransaction::Yes),
        Context_->GetClient()->GetTransactionManager(),
        Request_->Path,
        Request_->Path.Attributes().Find<TKeyColumns>("sorted_by"));

    writer->Open();

    TTableConsumer consumer(writer);

    auto format = Context_->GetInputFormat();
    auto parser = CreateParserForFormat(format, EDataType::Tabular, &consumer);

    struct TWriteBufferTag { };
    auto buffer = TSharedRef::Allocate<TWriteBufferTag>(config->BlockSize);

    auto input = Context_->Request().InputStream;

    while (true) {
        auto bytesRead = WaitFor(input->Read(buffer.Begin(), buffer.Size()));
        THROW_ERROR_EXCEPTION_IF_FAILED(bytesRead);

        if (bytesRead.Value() == 0)
            break;

        parser->Read(TStringBuf(buffer.Begin(), bytesRead.Value()));

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
    if (Request_->FirstTabletIndex) {
        options.FirstTabletIndex = *Request_->FirstTabletIndex;
    }
    if (Request_->LastTabletIndex) {
        options.LastTabletIndex = *Request_->LastTabletIndex;
    }

    auto result = WaitFor(Context_->GetClient()->MountTable(
        Request_->Path.GetPath(),
        options));
    THROW_ERROR_EXCEPTION_IF_FAILED(result);
}

////////////////////////////////////////////////////////////////////////////////

void TUnmountTableCommand::DoExecute()
{
    TUnmountTableOptions options;
    if (Request_->FirstTabletIndex) {
        options.FirstTabletIndex = *Request_->FirstTabletIndex;
    }
    if (Request_->LastTabletIndex) {
        options.LastTabletIndex = *Request_->LastTabletIndex;
    }
    options.Force = Request_->Force;

    auto result = WaitFor(Context_->GetClient()->UnmountTable(
        Request_->Path.GetPath(),
        options));
    THROW_ERROR_EXCEPTION_IF_FAILED(result);
}

////////////////////////////////////////////////////////////////////////////////

void TRemountTableCommand::DoExecute()
{
    TRemountTableOptions options;
    if (Request_->FirstTabletIndex) {
        options.FirstTabletIndex = *Request_->FirstTabletIndex;
    }
    if (Request_->LastTabletIndex) {
        options.LastTabletIndex = *Request_->LastTabletIndex;
    }

    auto result = WaitFor(Context_->GetClient()->RemountTable(
        Request_->Path.GetPath(),
        options));
    THROW_ERROR_EXCEPTION_IF_FAILED(result);
}

////////////////////////////////////////////////////////////////////////////////

void TReshardTableCommand::DoExecute()
{
    TReshardTableOptions options;
    if (Request_->FirstTabletIndex) {
        options.FirstTabletIndex = *Request_->FirstTabletIndex;
    }
    if (Request_->LastTabletIndex) {
        options.LastTabletIndex = *Request_->LastTabletIndex;
    }
    
    std::vector<TUnversionedRow> pivotKeys;
    for (const auto& key : Request_->PivotKeys) {
        pivotKeys.push_back(key.Get());
    }
    
    auto result = WaitFor(Context_->GetClient()->ReshardTable(
        Request_->Path.GetPath(),
        pivotKeys,
        options));
    THROW_ERROR_EXCEPTION_IF_FAILED(result);
}

////////////////////////////////////////////////////////////////////////////////

void TInsertCommand::DoExecute()
{
    // COMPAT(babenko): remove Request_->TableWriter
    auto config = UpdateYsonSerializable(
        Context_->GetConfig()->TableWriter,
        Request_->TableWriter);
    config = UpdateYsonSerializable(
        config,
        Request_->GetOptions());

    auto tableMountCache = Context_->GetClient()->GetConnection()->GetTableMountCache();
    auto tableInfoOrError = WaitFor(tableMountCache->GetTableInfo(Request_->Path.GetPath()));
    THROW_ERROR_EXCEPTION_IF_FAILED(tableInfoOrError);
    const auto& tableInfo = tableInfoOrError.Value();

    // Parse input data.
    TBuildingTableConsumer consumer(
        tableInfo->Schema,
        tableInfo->KeyColumns);
    consumer.SetTreatMissingAsNull(!Request_->Update);
    consumer.SetAllowNonSchemaColumns(false);

    auto format = Context_->GetInputFormat();
    auto parser = CreateParserForFormat(format, EDataType::Tabular, &consumer);

    struct TWriteBufferTag { };
    auto buffer = TSharedRef::Allocate<TWriteBufferTag>(config->BlockSize);

    auto input = Context_->Request().InputStream;

    while (true) {
        auto bytesRead = WaitFor(input->Read(buffer.Begin(), buffer.Size()));
        THROW_ERROR_EXCEPTION_IF_FAILED(bytesRead);

        if (bytesRead.Value() == 0)
            break;

        parser->Read(TStringBuf(buffer.Begin(), bytesRead.Value()));
    }

    parser->Finish();

    // Write data into the tablets.

    auto transactionOrError = WaitFor(Context_->GetClient()->StartTransaction(ETransactionType::Tablet));
    THROW_ERROR_EXCEPTION_IF_FAILED(transactionOrError);
    auto transaction = transactionOrError.Value();

    // Convert to non-owning.
    std::vector<TUnversionedRow> rows;
    for (const auto& row : consumer.Rows()) {
        rows.emplace_back(row.Get());
    }

    transaction->WriteRows(
        Request_->Path.GetPath(),
        consumer.GetNameTable(),
        std::move(rows));

    auto commitResult = WaitFor(transaction->Commit());
    THROW_ERROR_EXCEPTION_IF_FAILED(commitResult);
}

////////////////////////////////////////////////////////////////////////////////

void TSelectCommand::DoExecute()
{
    TSelectRowsOptions options;
    options.Timestamp = Request_->Timestamp;
    options.InputRowLimit = Request_->InputRowLimit;
    options.OutputRowLimit = Request_->OutputRowLimit;

    auto format = Context_->GetOutputFormat();
    auto output = Context_->Request().OutputStream;
    auto writer = CreateSchemafulWriterForFormat(format, output);

    auto queryStatisticsOrError = WaitFor(Context_->GetClient()->SelectRows(
        Request_->Query,
        writer,
        options));
    THROW_ERROR_EXCEPTION_IF_FAILED(queryStatisticsOrError);

    const auto& statistics = queryStatisticsOrError.Value();
    
    LOG_INFO(
        "Query result statistics (RowsRead: %" PRIi64 ", RowsWritten: %" PRIi64 ", AsyncTime: %" PRIi64 ", SyncTime: %" PRIi64 ", IncompleteInput: %s, IncompleteOutput: %s)",
        statistics.RowsRead,
        statistics.RowsWritten,
        statistics.AsyncTime.MilliSeconds(),
        statistics.SyncTime.MilliSeconds(),
        ~FormatBool(statistics.IncompleteInput),
        ~FormatBool(statistics.IncompleteOutput));

    BuildYsonMapFluently(Context_->Request().ResponseParametersConsumer)
        .Item("rows_read").Value(statistics.RowsRead)
        .Item("rows_written").Value(statistics.RowsWritten)
        .Item("async_time").Value(statistics.AsyncTime)
        .Item("sync_time").Value(statistics.SyncTime)
        .Item("incomplete_input").Value(statistics.IncompleteInput)
        .Item("incomplete_output").Value(statistics.IncompleteOutput);
}

////////////////////////////////////////////////////////////////////////////////

void TLookupCommand::DoExecute()
{
    auto tableMountCache = Context_->GetClient()->GetConnection()->GetTableMountCache();
    auto tableInfoOrError = WaitFor(tableMountCache->GetTableInfo(Request_->Path.GetPath()));
    THROW_ERROR_EXCEPTION_IF_FAILED(tableInfoOrError);
    const auto& tableInfo = tableInfoOrError.Value();
    auto nameTable = TNameTable::FromSchema(tableInfo->Schema);

    TLookupRowsOptions options;
    options.Timestamp = Request_->Timestamp;
    if (Request_->ColumnNames) {
        options.ColumnFilter.All = false;
        for (const auto& name : *Request_->ColumnNames) {
            int id = nameTable->GetId(name);
            options.ColumnFilter.Indexes.push_back(id);
        }
    }

    auto lookupResult = WaitFor(Context_->GetClient()->LookupRow(
        Request_->Path.GetPath(),
        nameTable,
        Request_->Key.Get(),
        options));
    THROW_ERROR_EXCEPTION_IF_FAILED(lookupResult);
    
    auto rowset = lookupResult.Value();
    auto format = Context_->GetOutputFormat();
    auto output = Context_->Request().OutputStream;
    auto writer = CreateSchemafulWriterForFormat(format, output);

    {
        auto result = WaitFor(writer->Open(rowset->GetSchema()));
        THROW_ERROR_EXCEPTION_IF_FAILED(result);
    }

    writer->Write(rowset->GetRows());

    {
        auto result = WaitFor(writer->Close());
        THROW_ERROR_EXCEPTION_IF_FAILED(result);
    }
}

////////////////////////////////////////////////////////////////////////////////

void TDeleteCommand::DoExecute()
{
    auto tableMountCache = Context_->GetClient()->GetConnection()->GetTableMountCache();
    auto tableInfoOrError = WaitFor(tableMountCache->GetTableInfo(Request_->Path.GetPath()));
    THROW_ERROR_EXCEPTION_IF_FAILED(tableInfoOrError);
    const auto& tableInfo = tableInfoOrError.Value();
    auto nameTable = TNameTable::FromKeyColumns(tableInfo->KeyColumns);

    auto transactionOrError = WaitFor(Context_->GetClient()->StartTransaction(ETransactionType::Tablet));
    THROW_ERROR_EXCEPTION_IF_FAILED(transactionOrError);
    auto transaction = transactionOrError.Value();

    transaction->DeleteRow(
        Request_->Path.GetPath(),
        nameTable,
        Request_->Key.Get());

    auto commitResult = WaitFor(transaction->Commit());
    THROW_ERROR_EXCEPTION_IF_FAILED(commitResult);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT
