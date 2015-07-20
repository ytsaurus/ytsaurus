#include "stdafx.h"
#include "table_commands.h"
#include "config.h"

#include <ytlib/new_table_client/helpers.h>
#include <ytlib/new_table_client/name_table.h>
#include <ytlib/new_table_client/schemaful_writer.h>
#include <ytlib/new_table_client/schemaless_chunk_reader.h>
#include <ytlib/new_table_client/schemaless_chunk_writer.h>
#include <ytlib/new_table_client/table_consumer.h>

#include <ytlib/tablet_client/table_mount_cache.h>

#include <ytlib/query_client/query_statistics.h>

#include <ytlib/api/transaction.h>
#include <ytlib/api/rowset.h>

namespace NYT {
namespace NDriver {

using namespace NYson;
using namespace NYTree;
using namespace NFormats;
using namespace NChunkClient;
using namespace NQueryClient;
using namespace NConcurrency;
using namespace NTransactionClient;
using namespace NHive;
using namespace NVersionedTableClient;
using namespace NApi;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = DriverLogger;

////////////////////////////////////////////////////////////////////////////////

void TReadTableCommand::DoExecute()
{
    // COMPAT(babenko): remove Request_->TableReader
    auto config = UpdateYsonSerializable(
        Context_->GetConfig()->TableReader,
        Request_->TableReader);

    config = UpdateYsonSerializable(
        config,
        Request_->GetOptions());

    auto readerOptions = New<TRemoteReaderOptions>();
    readerOptions->NetworkName = Context_->GetConfig()->NetworkName;

    auto options = TTableReaderOptions();
    options.Config = config;
    options.RemoteReaderOptions = readerOptions;

    options.TransactionId = Request_->TransactionId;
    options.Ping = true;
    options.PingAncestors = Request_->PingAncestors;

    auto reader = Context_->GetClient()->CreateTableReader(
        Request_->Path,
        options);

    WaitFor(reader->Open())
        .ThrowOnError();

    if (reader->GetTotalRowCount() > 0) {
        BuildYsonMapFluently(Context_->Request().ResponseParametersConsumer)
            .Item("start_row_index").Value(reader->GetTableRowIndex())
            .Item("approximate_row_count").Value(reader->GetTotalRowCount());
    } else {
        BuildYsonMapFluently(Context_->Request().ResponseParametersConsumer)
            .Item("approximate_row_count").Value(reader->GetTotalRowCount());
    }

    auto writer = CreateSchemalessWriterForFormat(
        Context_->GetOutputFormat(),
        reader->GetNameTable(),
        Context_->Request().OutputStream,
        false,
        false,
        0);

    PipeReaderToWriter(
        reader,
        writer,
        Request_->ControlAttributes,
        Context_->GetConfig()->ReadBufferRowCount);
}

//////////////////////////////////////////////////////////////////////////////////

void TWriteTableCommand::DoExecute()
{
    // COMPAT(babenko): remove Request_->TableWriter
    auto config = UpdateYsonSerializable(
        Context_->GetConfig()->TableWriter,
        Request_->TableWriter);
    config = UpdateYsonSerializable(
        config,
        Request_->GetOptions());

    auto options = New<TTableWriterOptions>();
    // NB: Other options are ignored.
    options->NetworkName = Context_->GetConfig()->NetworkName;

    auto keyColumns = Request_->Path.Attributes().Get<TKeyColumns>("sorted_by", TKeyColumns());
    auto nameTable = TNameTable::FromKeyColumns(keyColumns);

    auto writer = CreateSchemalessTableWriter(
        config,
        options,
        Request_->Path,
        nameTable,
        keyColumns,
        Context_->GetClient()->GetMasterChannel(EMasterChannelKind::Leader),
        AttachTransaction(false),
        Context_->GetClient()->GetTransactionManager());

    WaitFor(writer->Open())
        .ThrowOnError();

    auto writingConsumer = New<TWritingValueConsumer>(writer);
    TTableConsumer consumer(writingConsumer);

    TTableOutput output(Context_->GetInputFormat(), &consumer);
    auto input = CreateSyncAdapter(Context_->Request().InputStream);

    PipeInputToOutput(input.get(), &output, config->BlockSize);

    writingConsumer->Flush();

    WaitFor(writer->Close())
        .ThrowOnError();
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
    options.CellId = Request_->CellId;

    auto asyncResult = Context_->GetClient()->MountTable(
        Request_->Path.GetPath(),
        options);
    WaitFor(asyncResult)
        .ThrowOnError();
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

    auto asyncResult = Context_->GetClient()->UnmountTable(
        Request_->Path.GetPath(),
        options);
    WaitFor(asyncResult)
        .ThrowOnError();
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

    auto asyncResult = Context_->GetClient()->RemountTable(
        Request_->Path.GetPath(),
        options);
    WaitFor(asyncResult)
        .ThrowOnError();
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

    auto asyncResult = Context_->GetClient()->ReshardTable(
        Request_->Path.GetPath(),
        pivotKeys,
        options);
    WaitFor(asyncResult)
        .ThrowOnError();
}

////////////////////////////////////////////////////////////////////////////////

void TSelectRowsCommand::DoExecute()
{
    TSelectRowsOptions options;
    options.Timestamp = Request_->Timestamp;
    options.InputRowLimit = Request_->InputRowLimit;
    options.OutputRowLimit = Request_->OutputRowLimit;
    options.RangeExpansionLimit = Request_->RangeExpansionLimit;
    options.FailOnIncompleteResult = Request_->FailOnIncompleteResult;
    options.VerboseLogging = Request_->VerboseLogging;
    options.EnableCodeCache = Request_->EnableCodeCache;
    options.MaxSubqueries = Request_->MaxSubqueries;

    auto format = Context_->GetOutputFormat();
    auto output = Context_->Request().OutputStream;
    auto writer = CreateSchemafulWriterForFormat(format, output);

    auto asyncStatistics = Context_->GetClient()->SelectRows(
        Request_->Query,
        writer,
        options);
    auto statistics = WaitFor(asyncStatistics)
        .ValueOrThrow();

    LOG_INFO("Query result statistics (RowsRead: %v, RowsWritten: %v, AsyncTime: %v, SyncTime: %v, ExecuteTime: %v, "
        "ReadTime: %v, WriteTime: %v, IncompleteInput: %v, IncompleteOutput: %v)",
        statistics.RowsRead,
        statistics.RowsWritten,
        statistics.AsyncTime.MilliSeconds(),
        statistics.SyncTime.MilliSeconds(),
        statistics.ExecuteTime.MilliSeconds(),
        statistics.ReadTime.MilliSeconds(),
        statistics.WriteTime.MilliSeconds(),
        statistics.IncompleteInput,
        statistics.IncompleteOutput);

    BuildYsonMapFluently(Context_->Request().ResponseParametersConsumer)
        .Item("rows_read").Value(statistics.RowsRead)
        .Item("rows_written").Value(statistics.RowsWritten)
        .Item("async_time").Value(statistics.AsyncTime)
        .Item("sync_time").Value(statistics.SyncTime)
        .Item("execute_time").Value(statistics.ExecuteTime)
        .Item("read_time").Value(statistics.ReadTime)
        .Item("write_time").Value(statistics.WriteTime)
        .Item("codegen_time").Value(statistics.CodegenTime)
        .Item("incomplete_input").Value(statistics.IncompleteInput)
        .Item("incomplete_output").Value(statistics.IncompleteOutput);
}

////////////////////////////////////////////////////////////////////////////////

namespace {

std::vector<TUnversionedRow> ParseRows(
    ICommandContextPtr context,
    TTableWriterConfigPtr config,
    TBuildingValueConsumerPtr valueConsumer)
{
    TTableConsumer tableConsumer(valueConsumer);
    TTableOutput output(context->GetInputFormat(), &tableConsumer);
    auto input = CreateSyncAdapter(context->Request().InputStream);
    PipeInputToOutput(input.get(), &output, config->BlockSize);
    return valueConsumer->GetRows();
}

} // namespace

void TInsertRowsCommand::DoExecute()
{
    // COMPAT(babenko): remove Request_->TableWriter
    auto config = UpdateYsonSerializable(
        Context_->GetConfig()->TableWriter,
        Request_->TableWriter);
    config = UpdateYsonSerializable(
        config,
        Request_->GetOptions());

    auto tableMountCache = Context_->GetClient()->GetConnection()->GetTableMountCache();
    auto tableInfo = WaitFor(tableMountCache->GetTableInfo(Request_->Path.GetPath()))
        .ValueOrThrow();

    // Parse input data.
    auto valueConsumer = New<TBuildingValueConsumer>(
        tableInfo->Schema,
        tableInfo->KeyColumns);
    valueConsumer->SetTreatMissingAsNull(!Request_->Update);
    auto rows = ParseRows(Context_, config, valueConsumer);

    // Run writes.
    auto asyncTransaction = Context_->GetClient()->StartTransaction(ETransactionType::Tablet);
    auto transaction = WaitFor(asyncTransaction)
        .ValueOrThrow();

    transaction->WriteRows(
        Request_->Path.GetPath(),
        valueConsumer->GetNameTable(),
        std::move(rows));

    WaitFor(transaction->Commit())
        .ThrowOnError();
}

////////////////////////////////////////////////////////////////////////////////

void TLookupRowsCommand::DoExecute()
{
    // COMPAT(babenko): remove Request_->TableWriter
    auto config = UpdateYsonSerializable(
        Context_->GetConfig()->TableWriter,
        Request_->TableWriter);
    config = UpdateYsonSerializable(
        config,
        Request_->GetOptions());

    auto tableMountCache = Context_->GetClient()->GetConnection()->GetTableMountCache();
    auto asyncTableInfo = tableMountCache->GetTableInfo(Request_->Path.GetPath());
    auto tableInfo = WaitFor(asyncTableInfo)
        .ValueOrThrow();
    auto nameTable = TNameTable::FromSchema(tableInfo->Schema);

    // Parse input data.
    auto valueConsumer = New<TBuildingValueConsumer>(
        tableInfo->Schema.TrimNonkeyColumns(tableInfo->KeyColumns),
        tableInfo->KeyColumns);
    auto keys = ParseRows(Context_, config, valueConsumer);

    // Run lookup.
    TLookupRowsOptions options;
    options.Timestamp = Request_->Timestamp;
    if (Request_->ColumnNames) {
        options.ColumnFilter.All = false;
        for (const auto& name : *Request_->ColumnNames) {
            int id = nameTable->GetId(name);
            options.ColumnFilter.Indexes.push_back(id);
        }
    }

    auto asyncRowset = Context_->GetClient()->LookupRows(
        Request_->Path.GetPath(),
        valueConsumer->GetNameTable(),
        std::move(keys),
        options);
    auto rowset = WaitFor(asyncRowset)
        .ValueOrThrow();

    auto format = Context_->GetOutputFormat();
    auto output = Context_->Request().OutputStream;
    auto writer = CreateSchemafulWriterForFormat(format, output);

    WaitFor(writer->Open(rowset->GetSchema()))
        .ThrowOnError();

    writer->Write(rowset->GetRows());

    WaitFor(writer->Close())
        .ThrowOnError();
}

////////////////////////////////////////////////////////////////////////////////

void TDeleteRowsCommand::DoExecute()
{
    // COMPAT(babenko): remove Request_->TableWriter
    auto config = UpdateYsonSerializable(
        Context_->GetConfig()->TableWriter,
        Request_->TableWriter);
    config = UpdateYsonSerializable(
        config,
        Request_->GetOptions());

    auto tableMountCache = Context_->GetClient()->GetConnection()->GetTableMountCache();
    auto asyncTableInfo = tableMountCache->GetTableInfo(Request_->Path.GetPath());
    auto tableInfo = WaitFor(asyncTableInfo)
        .ValueOrThrow();

    // Parse input data.
    auto valueConsumer = New<TBuildingValueConsumer>(
        tableInfo->Schema.TrimNonkeyColumns(tableInfo->KeyColumns),
        tableInfo->KeyColumns);
    auto keys = ParseRows(Context_, config, valueConsumer);

    // Run deletes.
    auto asyncTransaction = Context_->GetClient()->StartTransaction(ETransactionType::Tablet);
    auto transaction = WaitFor(asyncTransaction)
        .ValueOrThrow();

    transaction->DeleteRows(
        Request_->Path.GetPath(),
        valueConsumer->GetNameTable(),
        std::move(keys));

    WaitFor(transaction->Commit())
        .ThrowOnError();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT
