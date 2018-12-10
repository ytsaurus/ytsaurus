#include "table_commands.h"
#include "config.h"

#include <yt/client/api/rowset.h>
#include <yt/client/api/transaction.h>
#include <yt/client/api/skynet.h>
#include <yt/client/api/table_reader.h>

#include <yt/ytlib/api/native/table_reader.h>

#include <yt/client/query_client/query_statistics.h>

#include <yt/client/table_client/name_table.h>
#include <yt/client/table_client/row_buffer.h>
#include <yt/client/table_client/schemaful_writer.h>
#include <yt/client/table_client/versioned_writer.h>
#include <yt/client/table_client/columnar_statistics.h>

#include <yt/ytlib/table_client/helpers.h>
#include <yt/ytlib/table_client/schemaless_chunk_reader.h>
#include <yt/ytlib/table_client/schemaless_chunk_writer.h>
#include <yt/client/table_client/table_consumer.h>

#include <yt/client/tablet_client/table_mount_cache.h>

#include <yt/client/formats/config.h>
#include <yt/client/formats/parser.h>

#include <yt/core/concurrency/periodic_executor.h>
#include <yt/core/misc/finally.h>

namespace NYT {
namespace NDriver {

using namespace NYson;
using namespace NYTree;
using namespace NFormats;
using namespace NChunkClient;
using namespace NQueryClient;
using namespace NConcurrency;
using namespace NTransactionClient;
using namespace NHiveClient;
using namespace NTableClient;
using namespace NTabletClient;
using namespace NApi;

////////////////////////////////////////////////////////////////////////////////

TReadTableCommand::TReadTableCommand()
{
    RegisterParameter("path", Path);
    RegisterParameter("table_reader", TableReader)
        .Default(nullptr);
    RegisterParameter("control_attributes", ControlAttributes)
        .DefaultNew();
    RegisterParameter("unordered", Unordered)
        .Default(false);
    RegisterParameter("start_row_index_only", StartRowIndexOnly)
        .Default(false);

    RegisterPostprocessor([&] {
        Path = Path.Normalize();
    });
}

void TReadTableCommand::DoExecute(ICommandContextPtr context)
{
    LOG_DEBUG("Executing \"read_table\" command (Path: %v, Unordered: %v, StartRowIndexOnly: %v)",
        Path,
        Unordered,
        StartRowIndexOnly);

    Options.Ping = true;
    Options.Config = UpdateYsonSerializable(
        context->GetConfig()->TableReader,
        TableReader);

    if (StartRowIndexOnly) {
        Options.Config->WindowSize = 1;
        Options.Config->GroupSize = 1;
    }

    auto reader = WaitFor(context->GetClient()->CreateTableReader(
        Path,
        Options))
        .ValueOrThrow();

    if (reader->GetTotalRowCount() > 0) {
        ProduceResponseParameters(context, [&] (IYsonConsumer* consumer) {
            BuildYsonMapFragmentFluently(consumer)
                .Item("start_row_index").Value(reader->GetTableRowIndex())
                .Item("approximate_row_count").Value(reader->GetTotalRowCount());
        });
    } else {
        ProduceResponseParameters(context, [&] (IYsonConsumer* consumer) {
            BuildYsonMapFragmentFluently(consumer)
                .Item("approximate_row_count").Value(reader->GetTotalRowCount());
        });
    }

    if (StartRowIndexOnly) {
        return;
    }

    auto writer = CreateSchemalessWriterForFormat(
        context->GetOutputFormat(),
        reader->GetNameTable(),
        context->Request().OutputStream,
        false,
        ControlAttributes,
        0);

    auto finally = Finally([&] () {
        auto dataStatistics = reader->GetDataStatistics();
        LOG_DEBUG("Command statistics (RowCount: %v, WrittenSize: %v, "
            "ReadUncompressedDataSize: %v, ReadCompressedDataSize: %v)",
            dataStatistics.row_count(),
            writer->GetWrittenSize(),
            dataStatistics.uncompressed_data_size(),
            dataStatistics.compressed_data_size());
    });

    TPipeReaderToWriterOptions options;
    options.BufferRowCount = context->GetConfig()->ReadBufferRowCount;
    PipeReaderToWriter(
        reader,
        writer,
        options);
}

////////////////////////////////////////////////////////////////////////////////

TReadBlobTableCommand::TReadBlobTableCommand()
{
    RegisterParameter("path", Path);
    RegisterParameter("part_size", PartSize);
    RegisterParameter("table_reader", TableReader)
        .Default(nullptr);
    RegisterParameter("part_index_column_name", PartIndexColumnName)
        .Default();
    RegisterParameter("data_column_name", DataColumnName)
        .Default();
    RegisterParameter("start_part_index", StartPartIndex)
        .Default(0);
    RegisterParameter("offset", Offset)
        .Default(0);

    RegisterPostprocessor([&] {
        Path = Path.Normalize();
    });
}

void TReadBlobTableCommand::DoExecute(ICommandContextPtr context)
{
    if (Offset < 0) {
        THROW_ERROR_EXCEPTION("Offset must be nonnegative");
    }

    if (PartSize <= 0) {
        THROW_ERROR_EXCEPTION("Part size must be positive");
    }
    Options.Ping = true;

    auto config = UpdateYsonSerializable(
        context->GetConfig()->TableReader,
        TableReader);

    Options.Config = config;

    auto reader = WaitFor(context->GetClient()->CreateTableReader(
        Path,
        Options))
        .ValueOrThrow();

    auto input = NNative::CreateBlobTableReader(
        std::move(reader),
        PartIndexColumnName,
        DataColumnName,
        StartPartIndex,
        Offset,
        PartSize);

    auto output = context->Request().OutputStream;

    // TODO(ignat): implement proper Pipe* function.
    while (true) {
        auto block = WaitFor(input->Read())
            .ValueOrThrow();

        if (!block)
            break;

        WaitFor(output->Write(block))
            .ThrowOnError();
    }
}

////////////////////////////////////////////////////////////////////////////////

TLocateSkynetShareCommand::TLocateSkynetShareCommand()
{
    RegisterParameter("path", Path);

    RegisterPostprocessor([&] {
        Path = Path.Normalize();
    });
}

void TLocateSkynetShareCommand::DoExecute(ICommandContextPtr context)
{
    Options.Config = context->GetConfig()->TableReader;

    auto asyncSkynetPartsLocations = context->GetClient()->LocateSkynetShare(
        Path,
        Options);

    auto skynetPartsLocations = WaitFor(asyncSkynetPartsLocations);

    auto format = context->GetOutputFormat();
    auto syncOutputStream = CreateBufferedSyncAdapter(context->Request().OutputStream);

    auto consumer = CreateConsumerForFormat(
        format,
        EDataType::Structured,
        syncOutputStream.get());

    Serialize(*skynetPartsLocations.ValueOrThrow(), consumer.get());
    consumer->Flush();
}

////////////////////////////////////////////////////////////////////////////////

TWriteTableCommand::TWriteTableCommand()
{
    RegisterParameter("path", Path);
    RegisterParameter("table_writer", TableWriter)
        .Default(nullptr);
    RegisterParameter("max_row_buffer_size", MaxRowBufferSize)
        .Default(1_MB);
    RegisterPostprocessor([&] {
        Path = Path.Normalize();
    });
}

void TWriteTableCommand::DoExecute(ICommandContextPtr context)
{
    auto transaction = AttachTransaction(context, false);

    auto config = UpdateYsonSerializable(
        context->GetConfig()->TableWriter,
        TableWriter);

    // XXX(babenko): temporary workaround; this is how it actually works but not how it is intended to be.
    Options.PingAncestors = true;
    Options.Config = config;

    auto apiWriter = WaitFor(context->GetClient()->CreateTableWriter(
        Path,
        Options))
        .ValueOrThrow();

    auto schemalessWriter = CreateSchemalessFromApiWriterAdapter(std::move(apiWriter));

    TWritingValueConsumer valueConsumer(
        schemalessWriter,
        ConvertTo<TTypeConversionConfigPtr>(context->GetInputFormat().Attributes()),
        MaxRowBufferSize);

    std::vector<IValueConsumer*> valueConsumers(1, &valueConsumer);
    TTableOutput output(CreateParserForFormat(
        context->GetInputFormat(),
        valueConsumers,
        0));

    PipeInputToOutput(context->Request().InputStream, &output, config->BlockSize);

    WaitFor(valueConsumer.Flush())
        .ThrowOnError();

    WaitFor(schemalessWriter->Close())
        .ThrowOnError();

    ProduceEmptyOutput(context);
}

////////////////////////////////////////////////////////////////////////////////

TGetTableColumnarStatisticsCommand::TGetTableColumnarStatisticsCommand()
{
    RegisterParameter("paths", Paths);
    RegisterPostprocessor([&] {
        for (auto& path : Paths) {
            path = path.Normalize();
            const auto& columns = path.GetColumns();
            if (!columns) {
                THROW_ERROR_EXCEPTION("It is required to specify column selectors in YPath for getting columnar statistics")
                    << TErrorAttribute("path", path);
            }
        }
    });
}

void TGetTableColumnarStatisticsCommand::DoExecute(ICommandContextPtr context)
{
    Options.FetchChunkSpecConfig = context->GetConfig()->TableReader;
    Options.FetcherConfig = context->GetConfig()->Fetcher;

    std::vector<std::vector<TString>> allColumns;
    allColumns.reserve(Paths.size());
    for (int index = 0; index < Paths.size(); ++index) {
        allColumns.push_back(*Paths[index].GetColumns());
    }

    auto transaction = AttachTransaction(context, false);

    // NB(psushin): This keepalive is an ugly hack for a long-running command with structured output - YT-9713.
    // Remove once framing is implemented - YT-9838.
    static auto keepAliveSpace = TSharedRef::FromString(" ");
    auto keepAliveCallback = [context] () {
        auto error = WaitFor(context->Request().OutputStream->Write(keepAliveSpace));
        // Ignore errors here. If user closed connection, it must be handled on the upper layer.
        Y_UNUSED(error);
    };

    auto keepAliveExecutor = New<TPeriodicExecutor>(
        GetSyncInvoker(),
        BIND(keepAliveCallback),
        TDuration::MilliSeconds(100));
    keepAliveExecutor->Start();

    auto allStatistics = WaitFor(context->GetClient()->GetColumnarStatistics(Paths, Options))
        .ValueOrThrow();

    WaitFor(keepAliveExecutor->Stop())
        .ThrowOnError();

    YCHECK(allStatistics.size() == Paths.size());
    for (int index = 0; index < allStatistics.size(); ++index) {
        YCHECK(allColumns[index].size() == allStatistics[index].ColumnDataWeights.size());
    }

    auto producer = [&] (IYsonConsumer* consumer) {
        BuildYsonFluently(consumer)
            .DoList([&] (TFluentList fluent) {
                for (int index = 0; index < Paths.size(); ++index) {
                    const auto& columns = allColumns[index];
                    const auto& statistics = allStatistics[index];
                    fluent
                        .Item()
                        .BeginMap()
                            .Item("column_data_weights").DoMap([&] (TFluentMap fluent) {
                                for (int index = 0; index < columns.size(); ++index) {
                                    fluent
                                        .Item(columns[index]).Value(statistics.ColumnDataWeights[index]);
                                }
                            })
                            .DoIf(statistics.TimestampTotalWeight.operator bool(), [&] (TFluentMap fluent) {
                                fluent.Item("timestamp_total_weight").Value(statistics.TimestampTotalWeight);
                            })
                            .Item("legacy_chunks_data_weight").Value(statistics.LegacyChunkDataWeight)
                        .EndMap();
                }
            });
    };
    ProduceOutput(context, producer, producer);
}

////////////////////////////////////////////////////////////////////////////////

TMountTableCommand::TMountTableCommand()
{
    RegisterParameter("cell_id", Options.CellId)
        .Optional();
    RegisterParameter("freeze", Options.Freeze)
        .Optional();
    RegisterParameter("target_cell_ids", Options.TargetCellIds)
        .Optional();
}

void TMountTableCommand::DoExecute(ICommandContextPtr context)
{
    auto asyncResult = context->GetClient()->MountTable(
        Path.GetPath(),
        Options);
    WaitFor(asyncResult)
        .ThrowOnError();

    ProduceEmptyOutput(context);
}

////////////////////////////////////////////////////////////////////////////////

TUnmountTableCommand::TUnmountTableCommand()
{
    RegisterParameter("force", Options.Force)
        .Optional();
}

void TUnmountTableCommand::DoExecute(ICommandContextPtr context)
{
    auto asyncResult = context->GetClient()->UnmountTable(
        Path.GetPath(),
        Options);
    WaitFor(asyncResult)
        .ThrowOnError();

    ProduceEmptyOutput(context);
}

////////////////////////////////////////////////////////////////////////////////

void TRemountTableCommand::DoExecute(ICommandContextPtr context)
{
    auto asyncResult = context->GetClient()->RemountTable(
        Path.GetPath(),
        Options);
    WaitFor(asyncResult)
        .ThrowOnError();

    ProduceEmptyOutput(context);
}

////////////////////////////////////////////////////////////////////////////////

void TFreezeTableCommand::DoExecute(ICommandContextPtr context)
{
    auto asyncResult = context->GetClient()->FreezeTable(
        Path.GetPath(),
        Options);
    WaitFor(asyncResult)
        .ThrowOnError();

    ProduceEmptyOutput(context);
}

////////////////////////////////////////////////////////////////////////////////

void TUnfreezeTableCommand::DoExecute(ICommandContextPtr context)
{
    auto asyncResult = context->GetClient()->UnfreezeTable(
        Path.GetPath(),
        Options);
    WaitFor(asyncResult)
        .ThrowOnError();

    ProduceEmptyOutput(context);
}

////////////////////////////////////////////////////////////////////////////////

TReshardTableCommand::TReshardTableCommand()
{
    RegisterParameter("pivot_keys", PivotKeys)
        .Default();
    RegisterParameter("tablet_count", TabletCount)
        .Default()
        .GreaterThan(0);

    RegisterPostprocessor([&] () {
        if (PivotKeys && TabletCount) {
            THROW_ERROR_EXCEPTION("Cannot specify both \"pivot_keys\" and \"tablet_count\"");
        }
        if (!PivotKeys && !TabletCount) {
            THROW_ERROR_EXCEPTION("Must specify either \"pivot_keys\" or \"tablet_count\"");
        }
    });
}

void TReshardTableCommand::DoExecute(ICommandContextPtr context)
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

    ProduceEmptyOutput(context);
}

////////////////////////////////////////////////////////////////////////////////

TAlterTableCommand::TAlterTableCommand()
{
    RegisterParameter("path", Path);
    RegisterParameter("schema", Options.Schema)
        .Optional();
    RegisterParameter("dynamic", Options.Dynamic)
        .Optional();
    RegisterParameter("upstream_replica_id", Options.UpstreamReplicaId)
        .Optional();
}

void TAlterTableCommand::DoExecute(ICommandContextPtr context)
{
    auto asyncResult = context->GetClient()->AlterTable(
        Path.GetPath(),
        Options);
    WaitFor(asyncResult)
        .ThrowOnError();

    ProduceEmptyOutput(context);
}

////////////////////////////////////////////////////////////////////////////////

TSelectRowsCommand::TSelectRowsCommand()
{
    RegisterParameter("query", Query);
    RegisterParameter("timestamp", Options.Timestamp)
        .Optional();
    RegisterParameter("input_row_limit", Options.InputRowLimit)
        .Optional();
    RegisterParameter("output_row_limit", Options.OutputRowLimit)
        .Optional();
    RegisterParameter("range_expansion_limit", Options.RangeExpansionLimit)
        .Optional();
    RegisterParameter("fail_on_incomplete_result", Options.FailOnIncompleteResult)
        .Optional();
    RegisterParameter("verbose_logging", Options.VerboseLogging)
        .Optional();
    RegisterParameter("enable_code_cache", Options.EnableCodeCache)
        .Optional();
    RegisterParameter("max_subqueries", Options.MaxSubqueries)
        .Optional();
    RegisterParameter("workload_descriptor", Options.WorkloadDescriptor)
        .Optional();
    RegisterParameter("use_multijoin", Options.UseMultijoin)
        .Optional();
    RegisterParameter("allow_full_scan", Options.AllowFullScan)
        .Optional();
    RegisterParameter("allow_join_without_index", Options.AllowJoinWithoutIndex)
        .Optional();
    RegisterParameter("udf_registry_path", Options.UdfRegistryPath)
        .Default(Null);
}

void TSelectRowsCommand::DoExecute(ICommandContextPtr context)
{
    auto clientBase = GetClientBase(context);
    auto result = WaitFor(clientBase->SelectRows(Query, Options))
        .ValueOrThrow();

    const auto& rowset = result.Rowset;
    const auto& statistics = result.Statistics;

    auto format = context->GetOutputFormat();
    auto output = context->Request().OutputStream;
    auto writer = CreateSchemafulWriterForFormat(format, rowset->Schema(), output);

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
}

////////////////////////////////////////////////////////////////////////////////

namespace {

std::vector<TUnversionedRow> ParseRows(
    ICommandContextPtr context,
    TTableWriterConfigPtr config,
    TBuildingValueConsumer* valueConsumer)
{
    std::vector<IValueConsumer*> valueConsumers(1, valueConsumer);
    TTableOutput output(CreateParserForFormat(
        context->GetInputFormat(),
        valueConsumers,
        0));

    auto input = CreateSyncAdapter(context->Request().InputStream);
    PipeInputToOutput(input.get(), &output, config->BlockSize);
    return valueConsumer->GetRows();
}

} // namespace

TInsertRowsCommand::TInsertRowsCommand()
{
    RegisterParameter("require_sync_replica", Options.RequireSyncReplica)
        .Optional();
    RegisterParameter("table_writer", TableWriter)
        .Default();
    RegisterParameter("path", Path);
    RegisterParameter("update", Update)
        .Default(false);
    RegisterParameter("aggregate", Aggregate)
        .Default(false);
}

void TInsertRowsCommand::DoExecute(ICommandContextPtr context)
{
    auto config = UpdateYsonSerializable(
        context->GetConfig()->TableWriter,
        TableWriter);

    auto tableMountCache = context->GetClient()->GetTableMountCache();
    auto tableInfo = WaitFor(tableMountCache->GetTableInfo(Path.GetPath()))
        .ValueOrThrow();

    tableInfo->ValidateDynamic();

    if (!tableInfo->IsSorted() && Update) {
        THROW_ERROR_EXCEPTION("Cannot use \"update\" mode for ordered tables");
    }

    struct TInsertRowsBufferTag
    { };

    // Parse input data.
    TBuildingValueConsumer valueConsumer(
        tableInfo->Schemas[ETableSchemaKind::Write],
        ConvertTo<TTypeConversionConfigPtr>(context->GetInputFormat().Attributes()));
    valueConsumer.SetAggregate(Aggregate);
    valueConsumer.SetTreatMissingAsNull(!Update);

    auto rows = ParseRows(context, config, &valueConsumer);
    auto rowBuffer = New<TRowBuffer>(TInsertRowsBufferTag());
    auto capturedRows = rowBuffer->Capture(rows);
    auto rowRange = MakeSharedRange(
        std::vector<TUnversionedRow>(capturedRows.begin(), capturedRows.end()),
        std::move(rowBuffer));

    // Run writes.
    auto transaction = GetTransaction(context);

    transaction->WriteRows(
        Path.GetPath(),
        valueConsumer.GetNameTable(),
        std::move(rowRange),
        Options);

    if (ShouldCommitTransaction()) {
        WaitFor(transaction->Commit())
            .ThrowOnError();
    }
}

////////////////////////////////////////////////////////////////////////////////

TLookupRowsCommand::TLookupRowsCommand()
{
    RegisterParameter("table_writer", TableWriter)
        .Default();
    RegisterParameter("path", Path);
    RegisterParameter("column_names", ColumnNames)
        .Default();
    RegisterParameter("versioned", Versioned)
        .Default(false);
    RegisterParameter("retention_config", RetentionConfig)
        .Optional();
    RegisterParameter("timestamp", Options.Timestamp)
        .Optional();
    RegisterParameter("keep_missing_rows", Options.KeepMissingRows)
        .Optional();
}

void TLookupRowsCommand::DoExecute(ICommandContextPtr context)
{
    auto tableMountCache = context->GetClient()->GetTableMountCache();
    auto asyncTableInfo = tableMountCache->GetTableInfo(Path.GetPath());
    auto tableInfo = WaitFor(asyncTableInfo)
        .ValueOrThrow();

    tableInfo->ValidateDynamic();

    auto config = UpdateYsonSerializable(
        context->GetConfig()->TableWriter,
        TableWriter);

    struct TLookupRowsBufferTag
    { };

    // Parse input data.
    TBuildingValueConsumer valueConsumer(
        tableInfo->Schemas[ETableSchemaKind::Lookup],
        ConvertTo<TTypeConversionConfigPtr>(context->GetInputFormat().Attributes()));
    auto keys = ParseRows(context, config, &valueConsumer);
    auto rowBuffer = New<TRowBuffer>(TLookupRowsBufferTag());
    auto capturedKeys = rowBuffer->Capture(keys);
    auto mutableKeyRange = MakeSharedRange(std::move(capturedKeys), std::move(rowBuffer));
    auto keyRange = TSharedRange<TUnversionedRow>(
        static_cast<const TUnversionedRow*>(mutableKeyRange.Begin()),
        static_cast<const TUnversionedRow*>(mutableKeyRange.End()),
        mutableKeyRange.GetHolder());
    auto nameTable = valueConsumer.GetNameTable();

    if (ColumnNames) {
        TColumnFilter::TIndexes columnFilterIndexes;
        for (const auto& name : *ColumnNames) {
            auto maybeIndex = nameTable->FindId(name);
            if (!maybeIndex) {
                if (!tableInfo->Schemas[ETableSchemaKind::Primary].FindColumn(name)) {
                    THROW_ERROR_EXCEPTION("No such column %Qv",
                        name);
                }
                maybeIndex = nameTable->GetIdOrRegisterName(name);
            }
            columnFilterIndexes.push_back(*maybeIndex);
        }
        Options.ColumnFilter = TColumnFilter(std::move(columnFilterIndexes));
    }

    // Run lookup.
    auto format = context->GetOutputFormat();
    auto output = context->Request().OutputStream;

    auto clientBase = GetClientBase(context);

    if (Versioned) {
        TVersionedLookupRowsOptions versionedOptions;
        versionedOptions.ColumnFilter = Options.ColumnFilter;
        versionedOptions.KeepMissingRows = Options.KeepMissingRows;
        versionedOptions.Timestamp = Options.Timestamp;
        versionedOptions.RetentionConfig = RetentionConfig;
        auto asyncRowset = clientBase->VersionedLookupRows(Path.GetPath(), std::move(nameTable), std::move(keyRange), versionedOptions);
        auto rowset = WaitFor(asyncRowset)
            .ValueOrThrow();
        auto writer = CreateVersionedWriterForFormat(format, rowset->Schema(), output);
        writer->Write(rowset->GetRows());
        WaitFor(writer->Close())
            .ThrowOnError();
    } else {
        auto asyncRowset = clientBase->LookupRows(Path.GetPath(), std::move(nameTable), std::move(keyRange), Options);
        auto rowset = WaitFor(asyncRowset)
            .ValueOrThrow();

        auto writer = CreateSchemafulWriterForFormat(format, rowset->Schema(), output);
        writer->Write(rowset->GetRows());
        WaitFor(writer->Close())
            .ThrowOnError();
    }
}

////////////////////////////////////////////////////////////////////////////////

TGetInSyncReplicasCommand::TGetInSyncReplicasCommand()
{
    RegisterParameter("path", Path);
    RegisterParameter("timestamp", Options.Timestamp);
}

void TGetInSyncReplicasCommand::DoExecute(ICommandContextPtr context)
{
    auto tableMountCache = context->GetClient()->GetTableMountCache();
    auto asyncTableInfo = tableMountCache->GetTableInfo(Path.GetPath());
    auto tableInfo = WaitFor(asyncTableInfo)
        .ValueOrThrow();

    tableInfo->ValidateDynamic();
    tableInfo->ValidateReplicated();

    auto config = UpdateYsonSerializable(
        context->GetConfig()->TableWriter,
        TableWriter);

    struct TInSyncBufferTag
    { };

    // Parse input data.
    TBuildingValueConsumer valueConsumer(
        tableInfo->Schemas[ETableSchemaKind::Lookup],
        ConvertTo<TTypeConversionConfigPtr>(context->GetInputFormat().Attributes()));
    auto keys = ParseRows(context, config, &valueConsumer);
    auto rowBuffer = New<TRowBuffer>(TInSyncBufferTag());
    auto capturedKeys = rowBuffer->Capture(keys);
    auto mutableKeyRange = MakeSharedRange(std::move(capturedKeys), std::move(rowBuffer));
    auto keyRange = TSharedRange<TUnversionedRow>(
        static_cast<const TUnversionedRow*>(mutableKeyRange.Begin()),
        static_cast<const TUnversionedRow*>(mutableKeyRange.End()),
        mutableKeyRange.GetHolder());
    auto nameTable = valueConsumer.GetNameTable();

    auto asyncReplicas = context->GetClient()->GetInSyncReplicas(
        Path.GetPath(),
        std::move(nameTable),
        std::move(keyRange),
        Options);
    auto replicasResult = WaitFor(asyncReplicas);
    auto replicas = replicasResult.ValueOrThrow();
    context->ProduceOutputValue(BuildYsonStringFluently().List(replicas));
}

////////////////////////////////////////////////////////////////////////////////

TDeleteRowsCommand::TDeleteRowsCommand()
{
    RegisterParameter("require_sync_replica", Options.RequireSyncReplica)
        .Optional();
    RegisterParameter("table_writer", TableWriter)
        .Default();
    RegisterParameter("path", Path);
}

void TDeleteRowsCommand::DoExecute(ICommandContextPtr context)
{
    auto config = UpdateYsonSerializable(
        context->GetConfig()->TableWriter,
        TableWriter);

    auto tableMountCache = context->GetClient()->GetTableMountCache();
    auto asyncTableInfo = tableMountCache->GetTableInfo(Path.GetPath());
    auto tableInfo = WaitFor(asyncTableInfo)
        .ValueOrThrow();

    tableInfo->ValidateDynamic();

    struct TDeleteRowsBufferTag
    { };

    // Parse input data.
    TBuildingValueConsumer valueConsumer(
        tableInfo->Schemas[ETableSchemaKind::Delete],
        ConvertTo<TTypeConversionConfigPtr>(context->GetInputFormat().Attributes()));
    auto keys = ParseRows(context, config, &valueConsumer);
    auto rowBuffer = New<TRowBuffer>(TDeleteRowsBufferTag());
    auto capturedKeys = rowBuffer->Capture(keys);
    auto keyRange = MakeSharedRange(
        std::vector<TKey>(capturedKeys.begin(), capturedKeys.end()),
        std::move(rowBuffer));

    // Run deletes.
    auto transaction = GetTransaction(context);

    transaction->DeleteRows(
        Path.GetPath(),
        valueConsumer.GetNameTable(),
        std::move(keyRange),
        Options);

    if (ShouldCommitTransaction()) {
        WaitFor(transaction->Commit())
            .ThrowOnError();
    }

    ProduceEmptyOutput(context);
}

////////////////////////////////////////////////////////////////////////////////

TTrimRowsCommand::TTrimRowsCommand()
{
    RegisterParameter("path", Path);
    RegisterParameter("tablet_index", TabletIndex);
    RegisterParameter("trimmed_row_count", TrimmedRowCount);
}

void TTrimRowsCommand::DoExecute(ICommandContextPtr context)
{
    auto client = context->GetClient();
    auto asyncResult = client->TrimTable(
        Path.GetPath(),
        TabletIndex,
        TrimmedRowCount,
        Options);
    WaitFor(asyncResult)
        .ThrowOnError();

    ProduceEmptyOutput(context);
}

////////////////////////////////////////////////////////////////////////////////

TEnableTableReplicaCommand::TEnableTableReplicaCommand()
{
    RegisterParameter("replica_id", ReplicaId);
    Options.Enabled = true;
}

void TEnableTableReplicaCommand::DoExecute(ICommandContextPtr context)
{
    auto client = context->GetClient();
    auto asyncResult = client->AlterTableReplica(ReplicaId, Options);
    WaitFor(asyncResult)
        .ThrowOnError();

    ProduceEmptyOutput(context);
}

////////////////////////////////////////////////////////////////////////////////

TDisableTableReplicaCommand::TDisableTableReplicaCommand()
{
    RegisterParameter("replica_id", ReplicaId);
    Options.Enabled = false;
}

void TDisableTableReplicaCommand::DoExecute(ICommandContextPtr context)
{
    auto client = context->GetClient();
    auto asyncResult = client->AlterTableReplica(ReplicaId, Options);
    WaitFor(asyncResult)
        .ThrowOnError();

    ProduceEmptyOutput(context);
}

////////////////////////////////////////////////////////////////////////////////

TAlterTableReplicaCommand::TAlterTableReplicaCommand()
{
    RegisterParameter("replica_id", ReplicaId);
    RegisterParameter("enabled", Options.Enabled)
        .Optional();
    RegisterParameter("mode", Options.Mode)
        .Optional();
    RegisterParameter("preserve_timestamps", Options.PreserveTimestamps)
        .Optional();
    RegisterParameter("atomicity", Options.Atomicity)
        .Optional();
}

void TAlterTableReplicaCommand::DoExecute(ICommandContextPtr context)
{
    auto client = context->GetClient();
    auto asyncResult = client->AlterTableReplica(ReplicaId, Options);
    WaitFor(asyncResult)
        .ThrowOnError();

    ProduceEmptyOutput(context);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT
