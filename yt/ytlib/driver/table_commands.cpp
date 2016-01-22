#include "table_commands.h"
#include "config.h"

#include <yt/ytlib/api/rowset.h>
#include <yt/ytlib/api/transaction.h>

#include <yt/ytlib/query_client/query_statistics.h>

#include <yt/ytlib/table_client/helpers.h>
#include <yt/ytlib/table_client/name_table.h>
#include <yt/ytlib/table_client/schemaful_writer.h>
#include <yt/ytlib/table_client/schemaless_chunk_reader.h>
#include <yt/ytlib/table_client/schemaless_chunk_writer.h>
#include <yt/ytlib/table_client/table_consumer.h>

#include <yt/ytlib/tablet_client/table_mount_cache.h>

#include <yt/ytlib/formats/config.h>

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
using namespace NTableClient;
using namespace NApi;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = DriverLogger;

////////////////////////////////////////////////////////////////////////////////

void TReadTableCommand::Execute(ICommandContextPtr context)
{
    Options.Ping = true;

    // COMPAT(babenko): remove Request_->TableReader
    auto config = UpdateYsonSerializable(
        context->GetConfig()->TableReader,
        TableReader);

    config = UpdateYsonSerializable(
        config,
        GetOptions());

    Options.Config = config;

    auto reader = WaitFor(context->GetClient()->CreateTableReader(
        Path,
        Options))
        .ValueOrThrow();

    if (reader->GetTotalRowCount() > 0) {
        BuildYsonMapFluently(context->Request().ResponseParametersConsumer)
            .Item("start_row_index").Value(reader->GetTableRowIndex())
            .Item("approximate_row_count").Value(reader->GetTotalRowCount());
    } else {
        BuildYsonMapFluently(context->Request().ResponseParametersConsumer)
            .Item("approximate_row_count").Value(reader->GetTotalRowCount());
    }

    auto writer = CreateSchemalessWriterForFormat(
        context->GetOutputFormat(),
        reader->GetNameTable(),
        context->Request().OutputStream,
        false,
        ControlAttributes,
        0);

    PipeReaderToWriter(
        reader,
        writer,
        context->GetConfig()->ReadBufferRowCount);
}

//////////////////////////////////////////////////////////////////////////////////

void TWriteTableCommand::Execute(ICommandContextPtr context)
{
    auto transaction = AttachTransaction(context, false);

    // COMPAT(babenko): remove Request_->TableWriter
    auto config = UpdateYsonSerializable(
        context->GetConfig()->TableWriter,
        TableWriter);
    config = UpdateYsonSerializable(
        config,
        GetOptions());

    auto keyColumns = Path.Attributes().Get<TKeyColumns>("sorted_by", TKeyColumns());
    auto nameTable = TNameTable::FromKeyColumns(keyColumns);

    auto writer = CreateSchemalessTableWriter(
        config,
        New<TTableWriterOptions>(),
        Path,
        nameTable,
        keyColumns,
        context->GetClient(),
        transaction);

    WaitFor(writer->Open())
        .ThrowOnError();

    auto writingConsumer = New<TWritingValueConsumer>(writer);
    TTableConsumer consumer(writingConsumer);

    TTableOutput output(context->GetInputFormat(), &consumer);
    auto input = CreateSyncAdapter(context->Request().InputStream);

    PipeInputToOutput(input.get(), &output, config->BlockSize);

    writingConsumer->Flush();

    WaitFor(writer->Close())
        .ThrowOnError();
}

////////////////////////////////////////////////////////////////////////////////

void TMountTableCommand::Execute(ICommandContextPtr context)
{
    auto asyncResult = context->GetClient()->MountTable(
        Path.GetPath(),
        Options);
    WaitFor(asyncResult)
        .ThrowOnError();
}

////////////////////////////////////////////////////////////////////////////////

void TUnmountTableCommand::Execute(ICommandContextPtr context)
{
    auto asyncResult = context->GetClient()->UnmountTable(
        Path.GetPath(),
        Options);
    WaitFor(asyncResult)
        .ThrowOnError();
}

////////////////////////////////////////////////////////////////////////////////

void TRemountTableCommand::Execute(ICommandContextPtr context)
{
    auto asyncResult = context->GetClient()->RemountTable(
        Path.GetPath(),
        Options);
    WaitFor(asyncResult)
        .ThrowOnError();
}

////////////////////////////////////////////////////////////////////////////////

void TReshardTableCommand::Execute(ICommandContextPtr context)
{
    std::vector<TUnversionedRow> pivotKeys;
    for (const auto& key : PivotKeys) {
        pivotKeys.push_back(key);
    }

    auto asyncResult = context->GetClient()->ReshardTable(
        Path.GetPath(),
        pivotKeys,
        Options);
    WaitFor(asyncResult)
        .ThrowOnError();
}

////////////////////////////////////////////////////////////////////////////////

void TAlterTableCommand::Execute(ICommandContextPtr context)
{
    auto asyncResult = context->GetClient()->AlterTable(
        Path.GetPath(),
        Options);
    WaitFor(asyncResult)
        .ThrowOnError();
}

////////////////////////////////////////////////////////////////////////////////

void TSelectRowsCommand::Execute(ICommandContextPtr context)
{
    auto asyncResult = context->GetClient()->SelectRows(
        Query,
        Options);

    IRowsetPtr rowset;
    TQueryStatistics statistics;

    std::tie(rowset, statistics) = WaitFor(asyncResult)
        .ValueOrThrow();

    auto format = context->GetOutputFormat();
    auto output = context->Request().OutputStream;
    auto writer = CreateSchemafulWriterForFormat(format, rowset->GetSchema(), output);

    writer->Write(rowset->GetRows());

    WaitFor(writer->Close())
        .ThrowOnError();

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

    BuildYsonMapFluently(context->Request().ResponseParametersConsumer)
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

void TInsertRowsCommand::Execute(ICommandContextPtr context)
{
    TWriteRowsOptions writeOptions;
    writeOptions.Aggregate = Aggregate;

    // COMPAT(babenko): remove Request_->TableWriter
    auto config = UpdateYsonSerializable(
        context->GetConfig()->TableWriter,
        TableWriter);
    config = UpdateYsonSerializable(
        config,
        GetOptions());

    auto tableMountCache = context->GetClient()->GetConnection()->GetTableMountCache();
    auto tableInfo = WaitFor(tableMountCache->GetTableInfo(Path.GetPath()))
        .ValueOrThrow();

    tableInfo->ValidateDynamic();

    // Parse input data.
    auto valueConsumer = New<TBuildingValueConsumer>(
        tableInfo->Schema,
        tableInfo->KeyColumns);
    valueConsumer->SetTreatMissingAsNull(!Update);
    auto rows = ParseRows(context, config, valueConsumer);

    // Run writes.
    auto asyncTransaction = context->GetClient()->StartTransaction(ETransactionType::Tablet, Options);
    auto transaction = WaitFor(asyncTransaction)
        .ValueOrThrow();

    transaction->WriteRows(
        Path.GetPath(),
        valueConsumer->GetNameTable(),
        std::move(rows),
        writeOptions);

    WaitFor(transaction->Commit())
        .ThrowOnError();
}

////////////////////////////////////////////////////////////////////////////////

void TLookupRowsCommand::Execute(ICommandContextPtr context)
{
    auto tableMountCache = context->GetClient()->GetConnection()->GetTableMountCache();
    auto asyncTableInfo = tableMountCache->GetTableInfo(Path.GetPath());
    auto tableInfo = WaitFor(asyncTableInfo)
        .ValueOrThrow();
    tableInfo->ValidateDynamic();

    auto nameTable = TNameTable::FromSchema(tableInfo->Schema);

    if (ColumnNames) {
        Options.ColumnFilter.All = false;
        for (const auto& name : *ColumnNames) {
            auto maybeIndex = nameTable->FindId(name);
            if (!maybeIndex) {
                THROW_ERROR_EXCEPTION("No such column %Qv",
                    name);
            }
            Options.ColumnFilter.Indexes.push_back(*maybeIndex);
        }
    }

    // COMPAT(babenko): remove Request_->TableWriter
    auto config = UpdateYsonSerializable(
        context->GetConfig()->TableWriter,
        TableWriter);
    config = UpdateYsonSerializable(
        config,
        GetOptions());

    // Parse input data.
    auto valueConsumer = New<TBuildingValueConsumer>(
        tableInfo->Schema.TrimNonkeyColumns(tableInfo->KeyColumns),
        tableInfo->KeyColumns);
    auto keys = ParseRows(context, config, valueConsumer);

    // Run lookup.
    auto asyncRowset = context->GetClient()->LookupRows(
        Path.GetPath(),
        valueConsumer->GetNameTable(),
        std::move(keys),
        Options);
    auto rowset = WaitFor(asyncRowset)
        .ValueOrThrow();

    auto format = context->GetOutputFormat();
    auto output = context->Request().OutputStream;
    auto writer = CreateSchemafulWriterForFormat(format, rowset->GetSchema(), output);

    writer->Write(rowset->GetRows());

    WaitFor(writer->Close())
        .ThrowOnError();
}

////////////////////////////////////////////////////////////////////////////////

void TDeleteRowsCommand::Execute(ICommandContextPtr context)
{
    // COMPAT(babenko): remove Request_->TableWriter
    auto config = UpdateYsonSerializable(
        context->GetConfig()->TableWriter,
        TableWriter);
    config = UpdateYsonSerializable(
        config,
        GetOptions());

    auto tableMountCache = context->GetClient()->GetConnection()->GetTableMountCache();
    auto asyncTableInfo = tableMountCache->GetTableInfo(Path.GetPath());
    auto tableInfo = WaitFor(asyncTableInfo)
        .ValueOrThrow();
    tableInfo->ValidateDynamic();


    // Parse input data.
    auto valueConsumer = New<TBuildingValueConsumer>(
        tableInfo->Schema.TrimNonkeyColumns(tableInfo->KeyColumns),
        tableInfo->KeyColumns);
    auto keys = ParseRows(context, config, valueConsumer);

    // Run deletes.
    auto asyncTransaction = context->GetClient()->StartTransaction(ETransactionType::Tablet, Options);
    auto transaction = WaitFor(asyncTransaction)
        .ValueOrThrow();

    transaction->DeleteRows(
        Path.GetPath(),
        valueConsumer->GetNameTable(),
        std::move(keys));

    WaitFor(transaction->Commit())
        .ThrowOnError();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT
