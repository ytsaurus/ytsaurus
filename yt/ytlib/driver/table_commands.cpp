#include "table_commands.h"
#include "config.h"

#include <yt/ytlib/api/rowset.h>
#include <yt/ytlib/api/transaction.h>

#include <yt/ytlib/query_client/query_statistics.h>

#include <yt/ytlib/table_client/helpers.h>
#include <yt/ytlib/table_client/name_table.h>
#include <yt/ytlib/table_client/row_buffer.h>
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
using namespace NTabletClient;
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

    auto keyColumns = Path.GetSortedBy();
    auto nameTable = TNameTable::FromKeyColumns(keyColumns);

    auto options = New<TTableWriterOptions>();
    options->ValidateDuplicateIds = true;
    options->ValidateRowWeight = true;

    auto writer = CreateSchemalessTableWriter(
        config,
        options,
        Path,
        nameTable,
        keyColumns,
        context->GetClient(),
        transaction);

    WaitFor(writer->Open())
        .ThrowOnError();

    TWritingValueConsumer valueConsumer(writer);
    TTableConsumer tableConsumer(&valueConsumer);

    TTableOutput output(context->GetInputFormat(), &tableConsumer);
    auto input = CreateSyncAdapter(context->Request().InputStream);

    PipeInputToOutput(input.get(), &output, config->BlockSize);

    valueConsumer.Flush();

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
    TFuture<void> asyncResult;
    if (PivotKeys) {
        asyncResult = context->GetClient()->ReshardTable(
            Path.GetPath(),
            *PivotKeys,
            Options);
    } else {
        asyncResult = context->GetClient()->ReshardTable(
            Path.GetPath(),
            *TabletCount,
            Options);
    }
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
    TIntrusivePtr<IClientBase> clientBase = context->GetClient();
    {
        auto stickyTransaction = context->FindAndTouchTransaction(TransactionId);
        if (stickyTransaction) {
            clientBase = std::move(stickyTransaction);
        }
    }

    auto asyncResult = clientBase->SelectRows(Query, Options);

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
    TBuildingValueConsumer* valueConsumer)
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

    if (!tableInfo->IsSorted() && Update) {
        THROW_ERROR_EXCEPTION("Cannot use \"update\" mode for ordered tables");
    }

    struct TInsertRowsBufferTag
    { };

    // Parse input data.
    auto valueConsumer = New<TBuildingValueConsumer>(tableInfo->Schemas[ETableSchemaKind::Write]);
    valueConsumer->SetTreatMissingAsNull(!Update);

    auto rows = ParseRows(context, config, &valueConsumer);
    auto rowBuffer = New<TRowBuffer>(TInsertRowsBufferTag());
    auto capturedRows = rowBuffer->Capture(rows);
    auto mutableRowRange = MakeSharedRange(std::move(capturedRows), std::move(rowBuffer));
    // XXX(sandello): No covariance here yet.
    auto rowRange = TSharedRange<TUnversionedRow>(
        static_cast<const TUnversionedRow*>(mutableRowRange.Begin()),
        static_cast<const TUnversionedRow*>(mutableRowRange.End()),
        mutableRowRange.GetHolder());

    // Run writes.
    auto transaction = context->FindAndTouchTransaction(TransactionId);
    bool shouldCommit = false;
    if (!transaction) {
        transaction = WaitFor(context->GetClient()->StartTransaction(ETransactionType::Tablet, Options))
            .ValueOrThrow();
        shouldCommit = true;
    }

    transaction->WriteRows(
        Path.GetPath(),
        valueConsumer.GetNameTable(),
        std::move(rowRange),
        writeOptions);

    if (shouldCommit) {
        WaitFor(transaction->Commit())
            .ThrowOnError();
    }
}

////////////////////////////////////////////////////////////////////////////////

void TLookupRowsCommand::Execute(ICommandContextPtr context)
{
    auto tableMountCache = context->GetClient()->GetConnection()->GetTableMountCache();
    auto asyncTableInfo = tableMountCache->GetTableInfo(Path.GetPath());
    auto tableInfo = WaitFor(asyncTableInfo)
        .ValueOrThrow();
    tableInfo->ValidateDynamic();

    const auto& schema = tableInfo->Schemas[ETableSchemaKind::Primary];
    auto nameTable = TNameTable::FromSchema(schema);

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

    struct TLookupRowsBufferTag
    { };

    // Parse input data.
    auto valueConsumer = TBuildingValueConsumer(tableInfo->Schema);
    auto keys = ParseRows(context, config, &valueConsumer);
    auto rowBuffer = New<TRowBuffer>(TLookupRowsBufferTag());
    auto capturedKeys = rowBuffer->Capture(keys);
    auto mutableKeyRange = MakeSharedRange(std::move(capturedKeys), std::move(rowBuffer));
    auto keyRange = TSharedRange<TUnversionedRow>(
        static_cast<const TUnversionedRow*>(mutableKeyRange.Begin()),
        static_cast<const TUnversionedRow*>(mutableKeyRange.End()),
        mutableKeyRange.GetHolder());

    // Run lookup.
    TIntrusivePtr<IClientBase> clientBase = context->GetClient();
    {
        auto stickyTransaction = context->FindAndTouchTransaction(TransactionId);
        if (stickyTransaction) {
            clientBase = std::move(stickyTransaction);
        }
    }

    auto asyncRowset = clientBase->LookupRows(
        Path.GetPath(),
        valueConsumer.GetNameTable(),
        std::move(keyRange),
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

    struct TDeleteRowsBufferTag
    { };

    // Parse input data.
    const auto& schema = tableInfo->Schemas[ETableSchemaKind::Write];
    auto keyColumns = schema.GetKeyColumns();
    auto valueConsumer = New<TBuildingValueConsumer>(schema.TrimNonkeyColumns(keyColumns));
    auto keys = ParseRows(context, config, valueConsumer);
    auto rowBuffer = New<TRowBuffer>(TDeleteRowsBufferTag());
    auto capturedKeys = rowBuffer->Capture(keys);
    auto mutableKeyRange = MakeSharedRange(std::move(capturedKeys), std::move(rowBuffer));
    // XXX(sandello): No covariance here yet.
    auto keyRange = TSharedRange<TUnversionedRow>(
        static_cast<const TUnversionedRow*>(mutableKeyRange.Begin()),
        static_cast<const TUnversionedRow*>(mutableKeyRange.End()),
        mutableKeyRange.GetHolder());

    // Run deletes.
    auto transaction = context->FindAndTouchTransaction(TransactionId);
    bool shouldCommit = false;
    if (!transaction) {
        transaction = WaitFor(context->GetClient()->StartTransaction(ETransactionType::Tablet, Options))
            .ValueOrThrow();
        shouldCommit = true;
    }

    transaction->DeleteRows(
        Path.GetPath(),
        valueConsumer.GetNameTable(),
        std::move(keyRange));

    if (shouldCommit) {
        WaitFor(transaction->Commit())
            .ThrowOnError();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT
