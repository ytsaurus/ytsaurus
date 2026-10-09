#include "api_service_impl.h"

#include "query_corpus_reporter.h"

#include <yt/yt/ytlib/misc/memory_usage_tracker.h>

#include <yt/yt/client/api/transaction.h>

#include <yt/yt/client/chaos_client/replication_card_serialization.h>

#include <yt/yt/client/table_client/config.h>
#include <yt/yt/client/table_client/constrained_schema.h>
#include <yt/yt/client/table_client/helpers.h>
#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/row_buffer.h>
#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/wire_protocol.h>

#include <yt/yt/client/tablet_client/table_mount_cache.h>

#include <yt/yt/core/misc/serialize.h>

#include <library/cpp/yt/misc/cast.h>

#include <library/cpp/yt/string/string.h>

namespace NYT::NRpcProxy {

using namespace NApi::NRpcProxy;
using namespace NApi;
using namespace NChaosClient;
using namespace NCodegen;
using namespace NConcurrency;
using namespace NHydra;
using namespace NObjectClient;
using namespace NProfiling;
using namespace NQueryClient;
using namespace NRpc;
using namespace NTableClient;
using namespace NTabletClient;
using namespace NTracing;
using namespace NTransactionClient;
using namespace NYPath;
using namespace NYTree;
using namespace NYson;

using NYT::FromProto;
using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

void TMasterMetadataApiService::RegisterDynamicTableMethods()
{
    RegisterApiMethod(EMultiproxyMethodKind::Write, RPC_SERVICE_METHOD_DESC(MountTable));
    RegisterApiMethod(EMultiproxyMethodKind::Write, RPC_SERVICE_METHOD_DESC(UnmountTable));
    RegisterApiMethod(EMultiproxyMethodKind::Write, RPC_SERVICE_METHOD_DESC(RemountTable));
    RegisterApiMethod(EMultiproxyMethodKind::Write, RPC_SERVICE_METHOD_DESC(FreezeTable));
    RegisterApiMethod(EMultiproxyMethodKind::Write, RPC_SERVICE_METHOD_DESC(UnfreezeTable));
    RegisterApiMethod(EMultiproxyMethodKind::Write, RPC_SERVICE_METHOD_DESC(ReshardTable));
    RegisterApiMethod(EMultiproxyMethodKind::Write, RPC_SERVICE_METHOD_DESC(ReshardTableAutomatic));
    RegisterApiMethod(EMultiproxyMethodKind::Write, RPC_SERVICE_METHOD_DESC(AlterTable));
    RegisterApiMethod(EMultiproxyMethodKind::Write, RPC_SERVICE_METHOD_DESC(BalanceTabletCells));
    RegisterApiMethod(EMultiproxyMethodKind::Write, RPC_SERVICE_METHOD_DESC(TransferBundleResources));

    RegisterApiMethod(EMultiproxyMethodKind::Read, RPC_SERVICE_METHOD_DESC(GetTableMountInfo));
    RegisterApiMethod(EMultiproxyMethodKind::Read, RPC_SERVICE_METHOD_DESC(GetTablePivotKeys));
}

void TApiService::RegisterDynamicTableMethods()
{
    RegisterApiMethod(EMultiproxyMethodKind::Write, RPC_SERVICE_METHOD_DESC(TrimTable));
    RegisterApiMethod(EMultiproxyMethodKind::Write, RPC_SERVICE_METHOD_DESC(CreateTableBackup));
    RegisterApiMethod(EMultiproxyMethodKind::Write, RPC_SERVICE_METHOD_DESC(RestoreTableBackup));

    RegisterApiMethod(EMultiproxyMethodKind::Read, RPC_SERVICE_METHOD_DESC(LookupRows)
        .SetInvokerProvider(BIND(&TApiService::GetWorkerInvoker, Unretained(this))));
    RegisterApiMethod(EMultiproxyMethodKind::Read, RPC_SERVICE_METHOD_DESC(VersionedLookupRows)
        .SetInvokerProvider(BIND(&TApiService::GetWorkerInvoker, Unretained(this))));
    RegisterApiMethod(EMultiproxyMethodKind::Read, RPC_SERVICE_METHOD_DESC(MultiLookup)
        .SetInvokerProvider(BIND(&TApiService::GetWorkerInvoker, Unretained(this)))
        .SetConcurrencyLimit(1'000));
    RegisterApiMethod(EMultiproxyMethodKind::Read, RPC_SERVICE_METHOD_DESC(SelectRows)
        .SetInvokerProvider(BIND(&TApiService::GetWorkerInvoker, Unretained(this)))
        .SetCancelable(true));
    RegisterApiMethod(EMultiproxyMethodKind::Read, RPC_SERVICE_METHOD_DESC(ExplainQuery));
    RegisterApiMethod(EMultiproxyMethodKind::Write, RPC_SERVICE_METHOD_DESC(PullRows)
        .SetCancelable(true));
    RegisterApiMethod(EMultiproxyMethodKind::Read, RPC_SERVICE_METHOD_DESC(GetTabletInfos));
    RegisterApiMethod(EMultiproxyMethodKind::Read, RPC_SERVICE_METHOD_DESC(GetTabletErrors));

    RegisterApiMethod(EMultiproxyMethodKind::Write, RPC_SERVICE_METHOD_DESC(ModifyRows));
    RegisterApiMethod(EMultiproxyMethodKind::Write, RPC_SERVICE_METHOD_DESC(BatchModifyRows));
}

////////////////////////////////////////////////////////////////////////////////

DEFINE_RPC_SERVICE_METHOD(TMasterMetadataApiService, GetTableMountInfo)
{
    auto client = GetAuthenticatedClientOrThrow(context, request);

    auto path = FromProto<TYPath>(request->path());

    context->AnnotateRequest()
        .With("Path", path);

    const auto& tableMountCache = client->GetTableMountCache();
    ExecuteCall(
        context,
        [=] {
            return tableMountCache->GetTableInfo(path);
        },
        [] (const auto& context, const TTableMountInfoPtr& tableMountInfo) {
            auto* response = &context->Response();

            ToProto(response->mutable_table_id(), tableMountInfo->TableId);
            const auto& primarySchema = tableMountInfo->Schemas[ETableSchemaKind::Primary];
            ToProto(response->mutable_schema(), primarySchema);
            for (const auto& tabletInfoPtr : tableMountInfo->Tablets) {
                ToProto(response->add_tablets(), *tabletInfoPtr);
            }

            auto tabletCount = tableMountInfo->Tablets.size();
            if (tableMountInfo->IsChaosReplicated() && tableMountInfo->UpperCapBound.GetCount() != 0) {
                tabletCount = tableMountInfo->UpperCapBound[0].Data.Int64;
                response->set_tablet_count(tabletCount);
            }

            response->set_dynamic(tableMountInfo->Dynamic);
            ToProto(response->mutable_upstream_replica_id(), tableMountInfo->UpstreamReplicaId);
            for (const auto& replica : tableMountInfo->Replicas) {
                auto* protoReplica = response->add_replicas();
                ToProto(protoReplica->mutable_replica_id(), replica->ReplicaId);
                protoReplica->set_cluster_name(replica->ClusterName);
                protoReplica->set_replica_path(replica->ReplicaPath);
                protoReplica->set_mode(ToProto(replica->Mode));
            }
            response->set_physical_path(tableMountInfo->PhysicalPath);

            ToProto(response->mutable_indices(), tableMountInfo->Indices);

            context->AnnotateResponse()
                .With("Dynamic", tableMountInfo->Dynamic)
                .With("TabletCount", tabletCount)
                .With("ReplicaCount", tableMountInfo->Replicas.size());
        });
}

DEFINE_RPC_SERVICE_METHOD(TMasterMetadataApiService, GetTablePivotKeys)
{
    auto client = GetAuthenticatedClientOrThrow(context, request);

    auto path = FromProto<TYPath>(request->path());

    context->AnnotateRequest()
        .With("Path", path);
    TGetTablePivotKeysOptions options;
    options.RepresentKeyAsList = request->represent_key_as_list();

    ExecuteCall(
        context,
        [=] {
            return client->GetTablePivotKeys(path, options);
        },
        [] (const auto& context, const TYsonString& result) {
            auto* response = &context->Response();
            response->set_value(ToProto(result));
        });
}

DEFINE_RPC_SERVICE_METHOD(TMasterMetadataApiService, MountTable)
{
    auto client = GetAuthenticatedClientOrThrow(context, request);

    const auto& path = request->path();

    TMountTableOptions options;
    SetTimeoutOptions(&options, context.Get());
    SetMutatingOptions(&options, request, context.Get());
    if (request->has_cell_id()) {
        FromProto(&options.CellId, request->cell_id());
    }
    FromProto(&options.TargetCellIds, request->target_cell_ids());
    if (request->has_freeze()) {
        options.Freeze = request->freeze();
    }

    if (request->has_tablet_range_options()) {
        FromProto(&options, request->tablet_range_options());
    }

    context->AnnotateRequest()
        .With("Path", path);

    ExecuteCall(
        context,
        [=] {
            return client->MountTable(path, options);
        });
}

DEFINE_RPC_SERVICE_METHOD(TMasterMetadataApiService, UnmountTable)
{
    auto client = GetAuthenticatedClientOrThrow(context, request);

    const auto& path = request->path();

    TUnmountTableOptions options;
    SetTimeoutOptions(&options, context.Get());
    SetMutatingOptions(&options, request, context.Get());
    if (request->has_force()) {
        options.Force = request->force();
    }
    if (request->has_tablet_range_options()) {
        FromProto(&options, request->tablet_range_options());
    }

    context->AnnotateRequest()
        .With("Path", path);

    ExecuteCall(
        context,
        [=] {
            return client->UnmountTable(path, options);
        });
}

DEFINE_RPC_SERVICE_METHOD(TMasterMetadataApiService, RemountTable)
{
    auto client = GetAuthenticatedClientOrThrow(context, request);

    const auto& path = request->path();

    TRemountTableOptions options;
    SetTimeoutOptions(&options, context.Get());
    SetMutatingOptions(&options, request, context.Get());
    if (request->has_tablet_range_options()) {
        FromProto(&options, request->tablet_range_options());
    }

    context->AnnotateRequest()
        .With("Path", path);

    ExecuteCall(
        context,
        [=] {
            return client->RemountTable(path, options);
        });
}

DEFINE_RPC_SERVICE_METHOD(TMasterMetadataApiService, FreezeTable)
{
    auto client = GetAuthenticatedClientOrThrow(context, request);

    const auto& path = request->path();

    TFreezeTableOptions options;
    SetTimeoutOptions(&options, context.Get());
    SetMutatingOptions(&options, request, context.Get());
    if (request->has_tablet_range_options()) {
        FromProto(&options, request->tablet_range_options());
    }

    context->AnnotateRequest()
        .With("Path", path);

    ExecuteCall(
        context,
        [=] {
            return client->FreezeTable(path, options);
        });
}

DEFINE_RPC_SERVICE_METHOD(TMasterMetadataApiService, UnfreezeTable)
{
    auto client = GetAuthenticatedClientOrThrow(context, request);

    const auto& path = request->path();

    TUnfreezeTableOptions options;
    SetTimeoutOptions(&options, context.Get());
    SetMutatingOptions(&options, request, context.Get());
    if (request->has_tablet_range_options()) {
        FromProto(&options, request->tablet_range_options());
    }

    context->AnnotateRequest()
        .With("Path", path);

    ExecuteCall(
        context,
        [=] {
            return client->UnfreezeTable(path, options);
        });
}

DEFINE_RPC_SERVICE_METHOD(TMasterMetadataApiService, ReshardTable)
{
    auto client = GetAuthenticatedClientOrThrow(context, request);

    const auto& path = request->path();

    TReshardTableOptions options;
    SetTimeoutOptions(&options, context.Get());
    SetMutatingOptions(&options, request, context.Get());
    if (request->has_tablet_range_options()) {
        FromProto(&options, request->tablet_range_options());
    }
    if (request->has_uniform()) {
        options.Uniform = request->uniform();
    }
    if (request->has_enable_slicing()) {
        options.EnableSlicing = request->enable_slicing();
    }
    if (request->has_slicing_accuracy()) {
        options.SlicingAccuracy = request->slicing_accuracy();
    }

    TFuture<void> result;
    if (request->has_tablet_count()) {
        auto tabletCount = request->tablet_count();

        context->AnnotateRequest()
            .With("Path", path)
            .With("TabletCount", tabletCount);

        ExecuteCall(
            context,
            [=] {
                return client->ReshardTable(path, tabletCount, options);
            });
    } else {
        auto reader = CreateWireProtocolReader(MergeRefsToRef<TApiServiceBufferTag>(request->Attachments()));
        auto keyRange = reader->ReadUnversionedRowset(false);
        std::vector<TLegacyOwningKey> keys;
        keys.reserve(keyRange.Size());
        for (const auto& key : keyRange) {
            keys.emplace_back(key);
        }

        context->AnnotateRequest()
            .With("Path", path)
            .With("Keys", keys);

        ExecuteCall(
            context,
            [=] {
                return client->ReshardTable(path, keys, options);
            });
    }
}

DEFINE_RPC_SERVICE_METHOD(TMasterMetadataApiService, ReshardTableAutomatic)
{
    auto client = GetAuthenticatedClientOrThrow(context, request);

    const auto& path = request->path();
    auto keepActions = request->keep_actions();

    context->AnnotateRequest()
        .With("Path", path)
        .With("KeepActions", keepActions);

    TReshardTableAutomaticOptions options;
    SetTimeoutOptions(&options, context.Get());
    SetMutatingOptions(&options, request, context.Get());
    if (request->has_tablet_range_options()) {
        FromProto(&options, request->tablet_range_options());
    }
    options.KeepActions = keepActions;

    ExecuteCall(
        context,
        [=] {
            return client->ReshardTableAutomatic(path, options);
        },
        [] (const auto& context, const auto& tabletActions) {
            auto* response = &context->Response();
            ToProto(response->mutable_tablet_actions(), tabletActions);
            context->AnnotateResponse()
                .With("TabletActionIds", tabletActions);
        });
}

DEFINE_RPC_SERVICE_METHOD(TApiService, TrimTable)
{
    auto client = GetAuthenticatedClientOrThrow(context, request);

    const auto& path = request->path();
    auto tabletIndex = request->tablet_index();
    auto trimmedRowCount = request->trimmed_row_count();

    TTrimTableOptions options;
    SetTimeoutOptions(&options, context.Get());

    context->AnnotateRequest()
        .With("Path", path)
        .With("TabletIndex", tabletIndex)
        .With("TrimmedRowCount", trimmedRowCount);

    ExecuteCall(
        context,
        [=] {
            return client->TrimTable(
                path,
                tabletIndex,
                trimmedRowCount,
                options);
        });
}

DEFINE_RPC_SERVICE_METHOD(TMasterMetadataApiService, AlterTable)
{
    auto client = GetAuthenticatedClientOrThrow(context, request);

    const auto& path = request->path();

    TAlterTableOptions options;
    SetTimeoutOptions(&options, context.Get());
    SetMutatingOptions(&options, request, context.Get());
    if (request->has_schema()) {
        options.Schema = ConvertTo<TTableSchema>(TYsonString(request->schema()));
    }
    if (request->has_schema_id()) {
        options.SchemaId = FromProto<TMasterTableSchemaId>(request->schema_id());
    }
    if (request->has_constrained_schema()) {
        options.ConstrainedSchema = ConvertTo<TConstrainedTableSchema>(TYsonString(request->constrained_schema()));
    }
    if (request->has_constraints()) {
        options.Constraints = FromProto<TColumnNameToConstraintMap>(request->constraints());
    }
    if (request->has_dynamic()) {
        options.Dynamic = request->dynamic();
    }
    if (request->has_upstream_replica_id()) {
        options.UpstreamReplicaId = FromProto<TTableReplicaId>(request->upstream_replica_id());
    }
    if (request->has_transactional_options()) {
        FromProto(&options, request->transactional_options());
    }
    if (request->has_schema_modification()) {
        options.SchemaModification = FromProto<ETableSchemaModification>(request->schema_modification());
    }
    if (request->has_replication_progress()) {
        options.ReplicationProgress = FromProto<TReplicationProgress>(request->replication_progress());
    }
    if (request->has_clip_timestamp()) {
        options.ClipTimestamp = FromProto<NTransactionClient::TTimestamp>(request->clip_timestamp());
    }

    context->AnnotateRequest()
        .With("Path", path);

    ExecuteCall(
        context,
        [=] {
            return client->AlterTable(path, options);
        });
}

DEFINE_RPC_SERVICE_METHOD(TMasterMetadataApiService, BalanceTabletCells)
{
    auto client = GetAuthenticatedClientOrThrow(context, request);

    const auto& bundle = request->bundle();
    auto tables = FromProto<std::vector<NYPath::TYPath>>(request->movable_tables());
    bool keepActions = request->keep_actions();

    context->AnnotateRequest()
        .With("Bundle", bundle)
        .With("TablePaths", tables)
        .With("KeepActions", keepActions);

    TBalanceTabletCellsOptions options;
    SetTimeoutOptions(&options, context.Get());
    SetMutatingOptions(&options, request, context.Get());
    options.KeepActions = keepActions;

    ExecuteCall(
        context,
        [=] {
            return client->BalanceTabletCells(bundle, tables, options);
        },
        [] (const auto& context, const auto& tabletActions) {
            auto* response = &context->Response();
            ToProto(response->mutable_tablet_actions(), tabletActions);
            context->AnnotateResponse()
                .With("TabletActionIds", tabletActions);
        });
}

DEFINE_RPC_SERVICE_METHOD(TApiService, CreateTableBackup)
{
    auto client = GetAuthenticatedClientOrThrow(context, request);

    auto manifest = New<TBackupManifest>();
    FromProto(manifest.Get(), request->manifest());

    TCreateTableBackupOptions options;
    SetTimeoutOptions(&options, context.Get());
    options.CheckpointTimestampDelay = FromProto<TDuration>(request->checkpoint_timestamp_delay());
    options.CheckpointCheckPeriod = FromProto<TDuration>(request->checkpoint_check_period());
    options.CheckpointCheckTimeout = FromProto<TDuration>(request->checkpoint_check_timeout());
    options.Force = request->force();
    options.PreserveAccount = request->preserve_account();

    context->AnnotateRequest()
        .With("ClusterCount", manifest->Clusters.size())
        .With("CheckpointTimestampDelay", options.CheckpointTimestampDelay)
        .With("CheckpointCheckPeriod", options.CheckpointCheckPeriod)
        .With("CheckpointCheckTimeout", options.CheckpointCheckTimeout)
        .With("Force", options.Force)
        .With("PreserveAccount", options.PreserveAccount);

    ExecuteCall(
        context,
        [=] {
            return client->CreateTableBackup(manifest, options);
        });
}

DEFINE_RPC_SERVICE_METHOD(TApiService, RestoreTableBackup)
{
    auto client = GetAuthenticatedClientOrThrow(context, request);

    auto manifest = New<TBackupManifest>();
    FromProto(manifest.Get(), request->manifest());

    TRestoreTableBackupOptions options;
    SetTimeoutOptions(&options, context.Get());
    options.Force = request->force();
    options.Mount = request->mount();
    options.EnableReplicas = request->enable_replicas();
    options.PreserveAccount = request->preserve_account();

    context->AnnotateRequest()
        .With("ClusterCount", manifest->Clusters.size())
        .With("Force", options.Force)
        .With("Mount", options.Mount)
        .With("EnableReplicas", options.EnableReplicas)
        .With("PreserveAccount", options.PreserveAccount);

    ExecuteCall(
        context,
        [=] {
            return client->RestoreTableBackup(manifest, options);
        });
}

DEFINE_RPC_SERVICE_METHOD(TMasterMetadataApiService, TransferBundleResources)
{
    auto client = GetAuthenticatedClientOrThrow(context, request);

    auto srcBundle = request->src_bundle();
    auto dstBundle = request->dst_bundle();
    auto resourceDelta = ConvertToNode(TYsonString(request->resource_delta()));

    TTransferBundleResourcesOptions options;
    SetTimeoutOptions(&options, context.Get());
    SetMutatingOptions(&options, request, context.Get());

    context->AnnotateRequest()
        .With("SrcBundle", srcBundle)
        .With("DstBundle", dstBundle);

    ExecuteCall(
        context,
        [=] {
            return client->TransferBundleResources(srcBundle, dstBundle, resourceDelta, options);
        });
}

template <class TContext, class TRequest, class TOptions>
static void LookupRowsPrelude(
    const TIntrusivePtr<TContext>& context,
    const TRequest* request,
    TOptions* options)
{
    if (request->has_tablet_read_options()) {
        FromProto(options, request->tablet_read_options());
    }
    if (request->has_replica_consistency()) {
        options->ReplicaConsistency = FromProto<EReplicaConsistency>(request->replica_consistency());
    }

    SetTimeoutOptions(options, context.Get());

    options->Timestamp = FromProto<NTransactionClient::TTimestamp>(request->timestamp());

    if constexpr (requires { request->retention_timestamp(); }) {
        options->RetentionTimestamp = FromProto<NTransactionClient::TTimestamp>(request->retention_timestamp());
    }

    if (request->has_multiplexing_band()) {
        options->MultiplexingBand = FromProto<EMultiplexingBand>(request->multiplexing_band());
    }
}

template <class TContext, class TRequest>
static void LookupRowsPrologue(
    const TIntrusivePtr<TContext>& /*context*/,
    const TRequest* request,
    TNameTablePtr* nameTable,
    TSharedRange<TUnversionedRow>* keys,
    TLookupRequestOptions* options,
    const std::vector<TSharedRef>& attachments,
    const IMemoryUsageTrackerPtr& memoryTracker)
{
    if (attachments.empty()) {
        THROW_ERROR_EXCEPTION("Request is missing rowset in attachments");
    }

    struct TDeserializedRowsetTag { };
    auto rowBuffer = New<TRowBuffer>(TDeserializedRowsetTag());

    auto rowset = NApi::NRpcProxy::DeserializeRowset<TUnversionedRow>(
        request->rowset_descriptor(),
        MergeRefsToRef<TApiServiceBufferTag>(attachments),
        rowBuffer);

    auto guard = TMemoryUsageTrackerGuard::TryAcquire(memoryTracker, rowBuffer->GetCapacity())
        .ValueOrThrow();

    *nameTable = rowset->GetNameTable();
    *keys = MakeSharedRange(rowset->GetRows(), MakeSharedRangeHolder(rowset, std::move(guard)));

    TColumnFilter::TIndexes columnFilterIndexes;
    for (int i = 0; i < request->columns_size(); ++i) {
        columnFilterIndexes.push_back((*nameTable)->GetIdOrRegisterName(request->columns(i)));
    }
    options->ColumnFilter = request->columns_size() == 0
        ? TColumnFilter()
        : TColumnFilter(std::move(columnFilterIndexes));
    options->KeepMissingRows = request->keep_missing_rows();
    options->AllowMissingKeyColumns = request->allow_missing_key_columns();
    options->EnablePartialResult = request->enable_partial_result();
    if (request->has_use_lookup_cache()) {
        options->UseLookupCache = request->use_lookup_cache();
    }
    if constexpr (requires {request->has_versioned_read_options();}) {
        FromProto(&options->VersionedReadOptions, request->versioned_read_options());
    }
    if (request->has_execution_pool()) {
        options->ExecutionPool = request->execution_pool();
    }
}

void TApiService::ProcessLookupRowsDetailedProfilingInfo(
    TWallTimer timer,
    const std::string& userTag,
    const TDetailedProfilingInfoPtr& detailedProfilingInfo)
{
    TDetailedProfilingCountersPtr counters;
    if (detailedProfilingInfo->EnableDetailedTableProfiling) {
        counters = GetOrCreateDetailedProfilingCounters({
            .UserTag = userTag,
            .TablePath = detailedProfilingInfo->TablePath,
        });

        counters->LookupDurationTimer().Record(timer.GetElapsedTime());
        counters->LookupMountCacheWaitTimer().Record(detailedProfilingInfo->MountCacheWaitTime);
        counters->LookupPermissionCacheWaitTimer().Record(detailedProfilingInfo->PermissionCacheWaitTime);
    } else if (!detailedProfilingInfo->RetryReasons.empty() ||
        detailedProfilingInfo->WastedSubrequestCount > 0)
    {
        counters = GetOrCreateDetailedProfilingCounters({});
    }

    if (detailedProfilingInfo->WastedSubrequestCount > 0) {
        counters->WastedLookupSubrequestCount().Increment(detailedProfilingInfo->WastedSubrequestCount);
    }

    for (const auto& reason : detailedProfilingInfo->RetryReasons) {
        counters->GetRetryCounterByReason(reason)->Increment();
    }
}

void TApiService::ProcessSelectRowsDetailedProfilingInfo(
    TWallTimer timer,
    const std::string& userTag,
    const TDetailedProfilingInfoPtr& detailedProfilingInfo)
{
    TDetailedProfilingCountersPtr counters;
    if (detailedProfilingInfo->EnableDetailedTableProfiling) {
        counters = GetOrCreateDetailedProfilingCounters({
            .UserTag = userTag,
            .TablePath = detailedProfilingInfo->TablePath,
        });

        counters->SelectDurationTimer().Record(timer.GetElapsedTime());
        counters->SelectMountCacheWaitTimer().Record(detailedProfilingInfo->MountCacheWaitTime);
        counters->SelectPermissionCacheWaitTimer().Record(detailedProfilingInfo->PermissionCacheWaitTime);
    } else if (!detailedProfilingInfo->RetryReasons.empty()) {
        counters = GetOrCreateDetailedProfilingCounters({});
    }

    for (const auto& reason : detailedProfilingInfo->RetryReasons) {
        counters->GetRetryCounterByReason(reason)->Increment();
    }
}

DEFINE_RPC_SERVICE_METHOD(TApiService, LookupRows)
{
    auto client = GetAuthenticatedClientOrThrow(context, request);

    TWallTimer timer;

    const auto& path = request->path();

    TNameTablePtr nameTable;
    TSharedRange<TUnversionedRow> keys;

    TLookupRowsOptions options;
    auto detailedProfilingInfo = New<TDetailedProfilingInfo>();
    options.DetailedProfilingInfo = detailedProfilingInfo;

    LookupRowsPrelude(
        context,
        request,
        &options);
    LookupRowsPrologue(
        context,
        request,
        &nameTable,
        &keys,
        &options,
        request->Attachments(),
        HeavyRequestMemoryUsageTracker_);

    context->AnnotateRequest()
        .With("Path", request->path())
        .With("RowCount", keys.Size())
        .With("Timestamp", options.Timestamp)
        .With("ReplicaConsistency", options.ReplicaConsistency);
    NTracing::AnnotateTraceContext([&] (const auto& traceContext) {
        traceContext->AddTag("yt.table_path", path);
    });

    ExecuteCall(
        context,
        [=] {
            return client->LookupRows(
                path,
                std::move(nameTable),
                std::move(keys),
                options);
        },
        [=, this, this_ = MakeStrong(this), detailedProfilingInfo = std::move(detailedProfilingInfo)]
        (const auto& context, const auto& result) {
            const auto& rowset = result.Rowset;

            auto* response = &context->Response();
            ToProto(response->mutable_unavailable_key_indexes(), result.UnavailableKeyIndexes);
            response->Attachments() = PrepareRowsetForAttachment(response, rowset, HeavyRequestMemoryUsageTracker_);

            ProcessLookupRowsDetailedProfilingInfo(
                timer,
                context->GetAuthenticationIdentity().UserTag,
                detailedProfilingInfo);

            context->AnnotateResponse()
                .With("RowCount", rowset->GetRows().Size())
                .With("DetailedTableProfilingEnabled", detailedProfilingInfo->EnableDetailedTableProfiling);
        });
}

DEFINE_RPC_SERVICE_METHOD(TApiService, VersionedLookupRows)
{
    auto client = GetAuthenticatedClientOrThrow(context, request);

    TWallTimer timer;

    const auto& path = request->path();

    TNameTablePtr nameTable;
    TSharedRange<TUnversionedRow> keys;

    TVersionedLookupRowsOptions options;
    auto detailedProfilingInfo = New<TDetailedProfilingInfo>();
    options.DetailedProfilingInfo = detailedProfilingInfo;

    LookupRowsPrelude(
        context,
        request,
        &options);
    LookupRowsPrologue(
        context,
        request,
        &nameTable,
        &keys,
        &options,
        request->Attachments(),
        HeavyRequestMemoryUsageTracker_);

    context->AnnotateRequest()
        .With("Path", request->path())
        .With("RowCount", keys.Size())
        .With("Timestamp", options.Timestamp)
        .With("ReplicaConsistency", options.ReplicaConsistency);
    NTracing::AnnotateTraceContext([&] (const auto& traceContext) {
        traceContext->AddTag("yt.table_path", path);
    });

    if (request->has_retention_config()) {
        options.RetentionConfig = New<TRetentionConfig>();
        FromProto(options.RetentionConfig.Get(), request->retention_config());
    }

    ExecuteCall(
        context,
        [=] {
            return client->VersionedLookupRows(
                path,
                std::move(nameTable),
                std::move(keys),
                options);
        },
        [=, this, this_ = MakeStrong(this), detailedProfilingInfo = std::move(detailedProfilingInfo)]
        (const auto& context, const auto& result) {
            const auto& rowset = result.Rowset;

            auto* response = &context->Response();
            ToProto(response->mutable_unavailable_key_indexes(), result.UnavailableKeyIndexes);
            response->Attachments() = PrepareRowsetForAttachment(response, rowset, HeavyRequestMemoryUsageTracker_);

            ProcessLookupRowsDetailedProfilingInfo(
                timer,
                context->GetAuthenticationIdentity().UserTag,
                detailedProfilingInfo);

            context->AnnotateResponse()
                .With("RowCount", rowset->GetRows().Size())
                .With("EnableDetailedTableProfiling", detailedProfilingInfo->EnableDetailedTableProfiling);
        });
}

DEFINE_RPC_SERVICE_METHOD(TApiService, MultiLookup)
{
    auto client = GetAuthenticatedClientOrThrow(context, request);

    TWallTimer timer;

    int subrequestCount = request->subrequests_size();

    TMultiLookupOptions options;
    LookupRowsPrelude(
        context,
        request,
        &options);

    std::vector<TMultiLookupSubrequest> subrequests;
    std::vector<TDetailedProfilingInfoPtr> profilingInfos;
    subrequests.reserve(subrequestCount);
    profilingInfos.reserve(subrequestCount);

    int beginAttachmentIndex = 0;
    for (int i = 0; i < subrequestCount; ++i) {
        const auto& protoSubrequest = request->subrequests(i);

        auto& subrequest = subrequests.emplace_back();
        subrequest.Path = protoSubrequest.path();

        profilingInfos.push_back(New<TDetailedProfilingInfo>());
        subrequest.Options.DetailedProfilingInfo = profilingInfos.back();

        int endAttachmentIndex = beginAttachmentIndex + protoSubrequest.attachment_count();
        if (endAttachmentIndex > std::ssize(request->Attachments())) {
            THROW_ERROR_EXCEPTION(
                NRpc::EErrorCode::ProtocolError,
                "Subrequest %v refers to non-existing attachment %v (out of %v)",
                i,
                endAttachmentIndex,
                request->Attachments().size());
        }
        std::vector<TSharedRef> attachments{
            request->Attachments().begin() + beginAttachmentIndex,
            request->Attachments().begin() + endAttachmentIndex};

        LookupRowsPrologue(
            context,
            &protoSubrequest,
            &subrequest.NameTable,
            &subrequest.Keys,
            &subrequest.Options,
            attachments,
            HeavyRequestMemoryUsageTracker_);

        beginAttachmentIndex = endAttachmentIndex;
    }
    if (beginAttachmentIndex != std::ssize(request->Attachments())) {
        THROW_ERROR_EXCEPTION(
            NRpc::EErrorCode::ProtocolError,
            "Total number of attachments is too large: expected %v, actual %v",
            beginAttachmentIndex,
            request->Attachments().size());
    }

    context->AnnotateRequest()
        .With("Timestamp", options.Timestamp)
        .With("ReplicaConsistency", options.ReplicaConsistency)
        .With("Subrequests", MakeFormattableView( subrequests, [&] (auto* builder, const TMultiLookupSubrequest& request) { builder->AppendFormat("{Path: %v, RowCount: %v}", request.Path, request.Keys.Size()); }));
    NTracing::AnnotateTraceContext([&] (const auto& traceContext) {
        auto tablePaths = JoinToString(
            subrequests,
            [] (TStringBuilderBase* builder, const TMultiLookupSubrequest& subrequest) {
                builder->AppendString(subrequest.Path);
            },
            ";"_sb);
        traceContext->AddTag("yt.table_paths", tablePaths);
    });

    ExecuteCall(
        context,
        [=] {
            return client->MultiLookupRows(
                std::move(subrequests),
                std::move(options));
        },
        [=, this, this_ = MakeStrong(this), profilingInfos = std::move(profilingInfos)]
        (const auto& context, const auto& results) {
            auto* response = &context->Response();

            YT_VERIFY(subrequestCount == std::ssize(results));

            std::vector<int> rowCounts;
            rowCounts.reserve(subrequestCount);
            for (const auto& result : results) {
                const auto& rowset = result.Rowset;
                auto* subresponse = response->add_subresponses();
                auto attachments = PrepareRowsetForAttachment(subresponse, rowset, HeavyRequestMemoryUsageTracker_);
                subresponse->set_attachment_count(attachments.size());
                ToProto(subresponse->mutable_unavailable_key_indexes(), result.UnavailableKeyIndexes);
                response->Attachments().insert(
                    response->Attachments().end(),
                    attachments.begin(),
                    attachments.end());
                rowCounts.push_back(rowset->GetRows().Size());
            }

            for (const auto& detailedProfilingInfo : profilingInfos) {
                ProcessLookupRowsDetailedProfilingInfo(
                    timer,
                    context->GetAuthenticationIdentity().UserTag,
                    detailedProfilingInfo);
            }

            context->AnnotateResponse()
                .With("RowCounts", rowCounts);
        });
}

template <class TRequest>
static void FillSelectRowsOptionsBaseFromRequest(const TRequest request, TSelectRowsOptionsBase* options)
{
    if (request->has_timestamp()) {
        options->Timestamp = FromProto<NTransactionClient::TTimestamp>(request->timestamp());
    }
    if (request->has_udf_registry_path()) {
        options->UdfRegistryPath = request->udf_registry_path();
    }
}

DEFINE_RPC_SERVICE_METHOD(TApiService, SelectRows)
{
    TWallTimer timer;

    auto client = GetAuthenticatedClientOrThrow(context, request);

    const auto& query = request->query();

    const auto config = Config_.Acquire();

    TSelectRowsOptions options;
    SetTimeoutOptions(&options, context.Get());
    FillSelectRowsOptionsBaseFromRequest(request, &options);

    if (request->has_input_row_limit()) {
        options.InputRowLimit = request->input_row_limit();
    }
    if (request->has_output_row_limit()) {
        options.OutputRowLimit = request->output_row_limit();
    }
    if (request->has_range_expansion_limit()) {
        options.RangeExpansionLimit = request->range_expansion_limit();
    }
    if (request->has_max_subqueries()) {
        options.MaxSubqueries = request->max_subqueries();
    }
    if (request->has_allow_full_scan()) {
        options.AllowFullScan = request->allow_full_scan();
    }
    if (request->has_allow_join_without_index()) {
        options.AllowJoinWithoutIndex = request->allow_join_without_index();
    }
    if (request->has_execution_pool()) {
        options.ExecutionPool = request->execution_pool();
    }
    if (request->has_fail_on_incomplete_result()) {
        options.FailOnIncompleteResult = request->fail_on_incomplete_result();
    }
    if (request->has_verbose_logging()) {
        options.VerboseLogging = request->verbose_logging();
    }
    if (request->has_new_range_inference()) {
        options.NewRangeInference = request->new_range_inference();
    }
    if (request->has_enable_code_cache()) {
        options.EnableCodeCache = request->enable_code_cache();
    }
    if (request->has_retention_timestamp()) {
        options.RetentionTimestamp = FromProto<NTransactionClient::TTimestamp>(request->retention_timestamp());
    }
    // TODO: Support WorkloadDescriptor
    if (request->has_memory_limit_per_node()) {
        options.MemoryLimitPerNode = request->memory_limit_per_node();
    }
    // TODO(lukyan): Move to FillSelectRowsOptionsBaseFromRequest
    if (request->has_suppressable_access_tracking_options()) {
        FromProto(&options, request->suppressable_access_tracking_options());
    }
    if (request->has_replica_consistency()) {
        options.ReplicaConsistency = FromProto<EReplicaConsistency>(request->replica_consistency());
    }
    if (request->has_placeholder_values()) {
        options.PlaceholderValues = NYson::TYsonString(request->placeholder_values());
    }
    if (request->has_use_canonical_null_relations()) {
        options.UseCanonicalNullRelations = request->use_canonical_null_relations();
    }
    if (request->has_merge_versioned_rows()) {
        options.MergeVersionedRows = request->merge_versioned_rows();
    }
    if (request->has_syntax_version()) {
        options.SyntaxVersion = request->syntax_version();
    }
    options.ExpressionBuilderVersion = YT_OPTIONAL_FROM_PROTO(*request, expression_builder_version);
    options.HyperLogLogPrecision = YT_OPTIONAL_FROM_PROTO(*request, hyper_log_log_precision);
    if (request->has_execution_backend()) {
        options.ExecutionBackend = CheckedEnumCast<EExecutionBackend>(request->execution_backend());
    }
    if (request->has_optimization_level()) {
        options.OptimizationLevel = CheckedEnumCast<EOptimizationLevel>(request->optimization_level());
    }
    if (request->has_versioned_read_options()) {
        FromProto(&options.VersionedReadOptions, request->versioned_read_options());
    }
    if (request->has_use_lookup_cache()) {
        options.UseLookupCache = request->use_lookup_cache();
    }
    if (request->has_min_row_count_per_subquery()) {
        options.MinRowCountPerSubquery = request->min_row_count_per_subquery();
    }
    if (request->has_rowset_processing_batch_size()) {
        options.RowsetProcessingBatchSize = request->rowset_processing_batch_size();
    }
    if (request->has_write_rowset_size()) {
        options.WriteRowsetSize = request->write_rowset_size();
    }
    if (request->has_max_join_batch_size()) {
        options.MaxJoinBatchSize = request->max_join_batch_size();
    }
    options.UseOrderByInJoinSubqueries = YT_OPTIONAL_FROM_PROTO(*request, use_order_by_in_join_subqueries);
    options.EnableParallelizeUnorderedGroupBy = YT_OPTIONAL_FROM_PROTO(*request, enable_parallelize_unordered_group_by);
    if (request->has_statistics_aggregation()) {
        options.StatisticsAggregation = CheckedEnumCast<EStatisticsAggregation>(request->statistics_aggregation());
    }
    if (request->has_read_from()) {
        options.ReadFrom = CheckedEnumCast<EPeerKind>(request->read_from());
    }

    auto detailedProfilingInfo = New<TDetailedProfilingInfo>();
    options.DetailedProfilingInfo = detailedProfilingInfo;
    int queryTruncateLimit = config->TruncatedQueryLengthForRequestInfo.value_or(std::numeric_limits<int>::max());

    if (options.PlaceholderValues) {
        context->AnnotateRequest()
            .With("Query", TTruncatedStringView(query, queryTruncateLimit))
            .With("Timestamp", options.Timestamp)
            .With("PlaceholderValues", options.PlaceholderValues);
        YT_TLOG_DEBUG("Untruncated select query")
            .With("Query", query)
            .With("Timestamp", options.Timestamp)
            .With("PlaceholderValues", options.PlaceholderValues);
    } else {
        context->AnnotateRequest()
            .With("Query", TTruncatedStringView(query, queryTruncateLimit))
            .With("Timestamp", options.Timestamp);
        YT_TLOG_DEBUG("Untruncated select query")
            .With("Query", query)
            .With("Timestamp", options.Timestamp);
    }

    ExecuteCall(
        context,
        [=] {
            return client->SelectRows(query, options);
        },
        [=, this, this_ = MakeStrong(this), detailedProfilingInfo = std::move(detailedProfilingInfo)]
        (const auto& context, const auto& result) {
            auto* response = &context->Response();
            response->Attachments() = PrepareRowsetForAttachment(response, result.Rowset, HeavyRequestMemoryUsageTracker_);
            ToProto(response->mutable_statistics(), result.Statistics);

            ProcessSelectRowsDetailedProfilingInfo(
                timer,
                context->GetAuthenticationIdentity().UserTag,
                detailedProfilingInfo);

            auto rows = result.Rowset->GetRows();

            context->AnnotateResponse()
                .With("RowCount", rows.Size());

            SelectConsumeDataWeight_.Increment(result.Statistics.DataWeightRead.GetTotal());
            SelectConsumeRowCount_.Increment(result.Statistics.RowsRead.GetTotal());
            SelectOutputDataWeight_.Increment(GetDataWeight(rows));
            SelectOutputRowCount_.Increment(rows.Size());

            if (QueryCorpusReporter_) {
                QueryCorpusReporter_->AddQuery(query);
            }
        });
}

DEFINE_RPC_SERVICE_METHOD(TApiService, PullRows)
{
    auto client = GetAuthenticatedClientOrThrow(context, request);
    const auto& path = request->path();

    TPullRowsOptions options;
    FromProto(&options.UpstreamReplicaId, request->upstream_replica_id());
    options.TabletRowsPerRead = request->tablet_rows_per_read();
    options.OrderRowsByTimestamp = request->order_rows_by_timestamp();
    FromProto(&options.ReplicationProgress, request->replication_progress());
    if (request->has_upper_timestamp()) {
        options.UpperTimestamp = FromProto<NTransactionClient::TTimestamp>(request->upper_timestamp());
    }
    for (auto protoReplicationRowIndex : request->start_replication_row_indexes()) {
        auto tabletId = FromProto<TTabletId>(protoReplicationRowIndex.tablet_id());
        int rowIndex = protoReplicationRowIndex.row_index();
        if (options.StartReplicationRowIndexes.contains(tabletId)) {
            THROW_ERROR_EXCEPTION("Duplicate tablet id in start replication row indexes")
                .With("tablet_id", tabletId);
        }
        InsertOrCrash(options.StartReplicationRowIndexes, std::pair(tabletId, rowIndex));
    }

    context->AnnotateRequest()
        .With("ReplicationProgress", options.ReplicationProgress)
        .With("OrderRowsByTimestamp", options.OrderRowsByTimestamp)
        .With("UpperTimestamp", options.UpperTimestamp);

    ExecuteCall(
        context,
        [=] {
            return client->PullRows(path, options);
        },
        [=] (const auto& context, const auto& result) {
            auto* response = &context->Response();
            response->set_row_count(result.RowCount);
            response->set_data_weight(result.DataWeight);
            response->set_versioned(result.Versioned);
            ToProto(response->mutable_replication_progress(), result.ReplicationProgress);

            for (auto [tabletId, rowIndex] : result.EndReplicationRowIndexes) {
                auto* protoReplicationRowIndex = response->add_end_replication_row_indexes();
                ToProto(protoReplicationRowIndex->mutable_tablet_id(), tabletId);
                protoReplicationRowIndex->set_row_index(rowIndex);
            }

            response->Attachments() = NApi::NRpcProxy::SerializeRowset(
                *result.Rowset->GetSchema(),
                result.Rowset->GetRows(),
                response->mutable_rowset_descriptor(),
                result.Versioned);

            context->AnnotateResponse()
                .With("RowCount", result.RowCount);
        });
}

DEFINE_RPC_SERVICE_METHOD(TApiService, ExplainQuery)
{
    auto client = GetAuthenticatedClientOrThrow(context, request);

    const auto& query = request->query();

    TExplainQueryOptions options;
    SetTimeoutOptions(&options, context.Get());
    FillSelectRowsOptionsBaseFromRequest(request, &options);

    if (request->has_new_range_inference()) {
        options.NewRangeInference = request->new_range_inference();
    }

    if (request->has_syntax_version()) {
        options.SyntaxVersion = request->syntax_version();
    }

    context->AnnotateRequest()
        .With("Query", query)
        .With("Timestamp", options.Timestamp);

    ExecuteCall(
        context,
        [=] {
            return client->ExplainQuery(query, options);
        },
        [] (const auto& context, const auto& result) {
            auto* response = &context->Response();
            response->set_value(ToProto(result));
        });
}

DEFINE_RPC_SERVICE_METHOD(TApiService, GetTabletInfos)
{
    auto client = GetAuthenticatedClientOrThrow(context, request);

    const auto& path = request->path();
    auto tabletIndexes = FromProto<std::vector<int>>(request->tablet_indexes());

    context->AnnotateRequest()
        .With("Path", path)
        .With("TabletIndexes", tabletIndexes);

    TGetTabletInfosOptions options;
    SetTimeoutOptions(&options, context.Get());
    options.RequestErrors = request->request_errors();

    ExecuteCall(
        context,
        [=] {
            return client->GetTabletInfos(
                path,
                tabletIndexes,
                options);
        },
        [] (const auto& context, const auto& tabletInfos) {
            auto* response = &context->Response();
            for (const auto& tabletInfo : tabletInfos) {
                auto* protoTabletInfo = response->add_tablets();
                protoTabletInfo->set_total_row_count(tabletInfo.TotalRowCount);
                protoTabletInfo->set_trimmed_row_count(tabletInfo.TrimmedRowCount);
                YT_OPTIONAL_SET_PROTO(protoTabletInfo, flushed_row_count, tabletInfo.FlushedRowCount);
                protoTabletInfo->set_delayed_lockless_row_count(tabletInfo.DelayedLocklessRowCount);
                protoTabletInfo->set_barrier_timestamp(ToProto(tabletInfo.BarrierTimestamp));
                protoTabletInfo->set_last_write_timestamp(ToProto(tabletInfo.LastWriteTimestamp));
                ToProto(protoTabletInfo->mutable_tablet_errors(), tabletInfo.TabletErrors);

                if (tabletInfo.TableReplicaInfos) {
                    for (const auto& replicaInfo : *tabletInfo.TableReplicaInfos) {
                        auto* protoReplicaInfo = protoTabletInfo->add_replicas();
                        ToProto(protoReplicaInfo->mutable_replica_id(), replicaInfo.ReplicaId);
                        protoReplicaInfo->set_last_replication_timestamp(ToProto(replicaInfo.LastReplicationTimestamp));
                        protoReplicaInfo->set_mode(static_cast<NApi::NRpcProxy::NProto::ETableReplicaMode>(replicaInfo.Mode));
                        protoReplicaInfo->set_current_replication_row_index(replicaInfo.CurrentReplicationRowIndex);
                        protoReplicaInfo->set_committed_replication_row_index(replicaInfo.CommittedReplicationRowIndex);
                        ToProto(protoReplicaInfo->mutable_replication_error(), replicaInfo.ReplicationError);
                    }
                }
            }
        });
}

DEFINE_RPC_SERVICE_METHOD(TApiService, GetTabletErrors)
{
    auto client = GetAuthenticatedClientOrThrow(context, request);

    const auto& path = request->path();
    context->AnnotateRequest()
        .With("Path", path);

    TGetTabletErrorsOptions options;
    SetTimeoutOptions(&options, context.Get());
    if (request->has_limit()) {
        options.Limit = request->limit();
    }

    ExecuteCall(
        context,
        [=] {
            return client->GetTabletErrors(
                path,
                options);
        },
        [] (const auto& context, const auto& tabletErrors) {
            auto* response = &context->Response();
            for (const auto& [tabletId, errors] : tabletErrors.TabletErrors) {
                ToProto(response->add_tablet_ids(), tabletId);
                ToProto(response->add_tablet_errors()->mutable_errors(), errors);
            }

            for (const auto& [replicaId, errors] : tabletErrors.ReplicationErrors) {
                ToProto(response->add_replica_ids(), replicaId);
                ToProto(response->add_replication_errors()->mutable_errors(), errors);
            }
            if (tabletErrors.Incomplete) {
                response->set_incomplete(tabletErrors.Incomplete);
            }

            context->AnnotateResponse()
                .With("TabletErrorCount", tabletErrors.TabletErrors.size())
                .With("ReplicationErrorCount", tabletErrors.ReplicationErrors.size())
                .With("Incomplete", tabletErrors.Incomplete);
        });
}

void TApiService::DoModifyRows(
    const NApi::NRpcProxy::NProto::TReqModifyRows& request,
    const std::vector<TSharedRef>& attachments,
    const ITransactionPtr& transaction)
{
    const auto& path = request.path();

    IUnversionedRowsetPtr rowset;
    try {
        rowset = NApi::NRpcProxy::DeserializeRowset<TUnversionedRow>(
            request.rowset_descriptor(),
            MergeRefsToRef<TApiServiceBufferTag>(attachments));
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Error sending rows for table %v",
            path)
            .With(ex);
    }

    auto rowsetRows = rowset->GetRows();
    auto rowsetSize = std::ssize(rowset->GetRows());

    if (rowsetSize != request.row_modification_types_size()) {
        THROW_ERROR_EXCEPTION("Row count mismatch")
            .With("rowset_size", rowsetSize)
            .With("row_modification_types_size", request.row_modification_types_size());
    }

    auto totalLockCount = request.row_legacy_read_locks_size() + request.row_legacy_locks_size() + request.row_locks_size();
    if ((request.row_legacy_read_locks_size() != 0 && request.row_legacy_read_locks_size() != rowsetSize) ||
        (request.row_legacy_locks_size() != 0 && request.row_legacy_locks_size() != rowsetSize) ||
        (request.row_locks_size() != 0 && request.row_locks_size() != rowsetSize) ||
        (totalLockCount != 0 && totalLockCount != rowsetSize))
    {
        THROW_ERROR_EXCEPTION("Lock count mismatch")
            .With("rowset_size", rowsetSize)
            .With("row_legacy_read_locks_size", request.row_legacy_read_locks_size())
            .With("row_legacy_locks_size", request.row_legacy_locks_size())
            .With("row_locks_size", request.row_locks_size())
            .With("total_lock_count", totalLockCount);
    }

    std::vector<TRowModification> modifications;
    modifications.reserve(rowsetSize);
    for (ssize_t index = 0; index < rowsetSize; ++index) {
        TLockMask lockMask;
        if (index < request.row_legacy_read_locks_size()) {
            TLegacyLockBitmap readLockMask = request.row_legacy_read_locks(index);
            for (int index = 0; index < TLegacyLockMask::MaxCount; ++index) {
                if (readLockMask & (1u << index)) {
                    lockMask.Set(index, ELockType::SharedWeak);
                }
            }
        } else if (index < request.row_legacy_locks_size()) {
            auto legacyLocks = TLegacyLockMask(request.row_legacy_locks(index));
            int lockedPrefixLength = legacyLocks.GetLockedPrefixLength();
            for (int index = 0; index < lockedPrefixLength; ++index) {
                lockMask.Set(index, legacyLocks.Get(index));
            }
        } else if (index < request.row_locks_size()) {
            FromProto(&lockMask, request.row_locks(index));
        }

        switch (request.row_modification_types(index)) {
            case NApi::NRpcProxy::NProto::ERowModificationType::RMT_WRITE:
                THROW_ERROR_EXCEPTION_IF(!lockMask.IsNone(),
                    "Cannot perform lock by \"write\" modification type; use \"write_and_lock\"");

                modifications.push_back(NRowModifications::TWriteRow(rowsetRows[index]));
                break;

            case NApi::NRpcProxy::NProto::ERowModificationType::RMT_DELETE:
                THROW_ERROR_EXCEPTION_IF(!lockMask.IsNone(),
                    "Cannot perform lock by \"delete\" modification type; use \"write_and_lock\"");

                modifications.push_back(NRowModifications::TDeleteRow(rowsetRows[index]));
                break;

            case NApi::NRpcProxy::NProto::ERowModificationType::RMT_MODIFY:
                modifications.push_back(NRowModifications::TWriteAndLockRow(rowsetRows[index], std::move(lockMask)));
                break;

            default:
                THROW_ERROR_EXCEPTION("Unknown modification type")
                    .With("row_modification_type", request.row_modification_types(index))
                    .With("index", index);
        }
    }

    TModifyRowsOptions options;
    if (request.has_require_sync_replica()) {
        options.RequireSyncReplica = request.require_sync_replica();
    }
    if (request.has_upstream_replica_id()) {
        FromProto(&options.UpstreamReplicaId, request.upstream_replica_id());
    }
    if (request.has_allow_missing_key_columns()) {
        options.AllowMissingKeyColumns = request.allow_missing_key_columns();
    }

    if (Config_.Acquire()->EnableModifyRowsRequestReordering &&
        request.has_sequence_number())
    {
        options.SequenceNumber = request.sequence_number();
        options.SequenceNumberSourceId = request.sequence_number_source_id();
    }

    transaction->ModifyRows(
        path,
        rowset->GetNameTable(),
        MakeSharedRange(std::move(modifications), rowset),
        options);
}

DEFINE_RPC_SERVICE_METHOD(TApiService, ModifyRows)
{
    auto client = GetAuthenticatedClientOrThrow(context, request);

    auto transactionId = FromProto<TTransactionId>(request->transaction_id());

    context->AnnotateRequest()
        .With("TransactionId", transactionId)
        .With("Path", request->path())
        .With("ModificationCount", request->row_modification_types_size());

    auto transaction = GetTransactionOrThrow(
        client,
        transactionId,
        /*options*/ std::nullopt,
        /*searchInPool*/ true);

    DoModifyRows(*request, request->Attachments(), transaction);

    context->Reply();
}

DEFINE_RPC_SERVICE_METHOD(TApiService, BatchModifyRows)
{
    auto client = GetAuthenticatedClientOrThrow(context, request);

    auto transactionId = FromProto<TTransactionId>(request->transaction_id());

    context->AnnotateRequest()
        .With("TransactionId", transactionId)
        .With("BatchSize", request->part_counts_size());

    i64 attachmentCount = request->Attachments().size();
    i64 expectedAttachmentCount = 0;
    for (int partCount : request->part_counts()) {
        if (partCount < 0) {
            THROW_ERROR_EXCEPTION("Received a negative part count")
                .With("part_count", partCount);
        }
        if (partCount >= attachmentCount) {
            THROW_ERROR_EXCEPTION("Part count is too large")
                .With("part_count", partCount);
        }
        expectedAttachmentCount += partCount + 1;
    }
    if (attachmentCount != expectedAttachmentCount) {
        THROW_ERROR_EXCEPTION("Attachment count mismatch")
            .With("actual_attachment_count", attachmentCount)
            .With("expected_attachment_count", expectedAttachmentCount);
    }

    auto transaction = GetTransactionOrThrow(
        client,
        FromProto<TTransactionId>(request->transaction_id()),
        /*options*/ std::nullopt,
        /*searchInPool*/ true);

    int attachmentIndex = 0;
    for (int partCount : request->part_counts()) {
        NApi::NRpcProxy::NProto::TReqModifyRows subrequest;
        if (!TryDeserializeProto(&subrequest, request->Attachments()[attachmentIndex])) {
            THROW_ERROR_EXCEPTION(NRpc::EErrorCode::ProtocolError, "Error deserializing subrequest");
        }
        ++attachmentIndex;
        std::vector<TSharedRef> attachments(
            request->Attachments().begin() + attachmentIndex,
            request->Attachments().begin() + attachmentIndex + partCount);
        DoModifyRows(subrequest, attachments, transaction);
        attachmentIndex += partCount;
    }

    context->Reply();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpcProxy
