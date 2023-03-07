#include "helpers.h"

#include <yt/client/api/rowset.h>

#include <yt/client/table_client/unversioned_row.h>
#include <yt/client/table_client/row_base.h>
#include <yt/client/table_client/row_buffer.h>
#include <yt/client/table_client/name_table.h>
#include <yt/client/table_client/schema.h>

#include <yt/client/tablet_client/table_mount_cache.h>
#include <yt/client/table_client/wire_protocol.h>

namespace NYT::NApi::NRpcProxy {

using namespace NTableClient;
using namespace NTabletClient;

////////////////////////////////////////////////////////////////////////////////

void ThrowUnimplemented(const TString& method)
{
    THROW_ERROR_EXCEPTION("Method %Qv is not implemented in RPC proxy",
        method);
}

////////////////////////////////////////////////////////////////////////////////

namespace NProto {

////////////////////////////////////////////////////////////////////////////////
// OPTIONS
////////////////////////////////////////////////////////////////////////////////

void ToProto(
    NProto::TTransactionalOptions* proto,
    const NApi::TTransactionalOptions& options)
{
    if (options.TransactionId) {
        ToProto(proto->mutable_transaction_id(), options.TransactionId);
    }
    proto->set_ping(options.Ping);
    proto->set_ping_ancestors(options.PingAncestors);
    proto->set_sticky(options.Sticky);
}

void ToProto(
    NProto::TPrerequisiteOptions* proto,
    const NApi::TPrerequisiteOptions& options)
{
    for (const auto& item : options.PrerequisiteTransactionIds) {
        auto* protoItem = proto->add_transactions();
        ToProto(protoItem->mutable_transaction_id(), item);
    }
    for (const auto& item : options.PrerequisiteRevisions) {
        auto* protoItem = proto->add_revisions();
        protoItem->set_path(item->Path);
        protoItem->set_revision(item->Revision);
        ToProto(protoItem->mutable_transaction_id(), item->TransactionId);
    }
}

void ToProto(
    NProto::TMasterReadOptions* proto,
    const NApi::TMasterReadOptions& options)
{
    proto->set_read_from(static_cast<NProto::EMasterReadKind>(options.ReadFrom));
    proto->set_success_expiration_time(NYT::ToProto<i64>(options.ExpireAfterSuccessfulUpdateTime));
    proto->set_failure_expiration_time(NYT::ToProto<i64>(options.ExpireAfterFailedUpdateTime));
    proto->set_cache_sticky_group_size(options.CacheStickyGroupSize);
}

void ToProto(
    NProto::TMutatingOptions* proto,
    const NApi::TMutatingOptions& options)
{
    ToProto(proto->mutable_mutation_id(), options.GetOrGenerateMutationId());
    proto->set_retry(options.Retry);
}

void ToProto(
    NProto::TSuppressableAccessTrackingOptions* proto,
    const NApi::TSuppressableAccessTrackingOptions& options)
{
    proto->set_suppress_access_tracking(options.SuppressAccessTracking);
    proto->set_suppress_modification_tracking(options.SuppressModificationTracking);
}

void ToProto(
    NProto::TTabletRangeOptions* proto,
    const NApi::TTabletRangeOptions& options)
{
    if (options.FirstTabletIndex) {
        proto->set_first_tablet_index(*options.FirstTabletIndex);
    }
    if (options.LastTabletIndex) {
        proto->set_last_tablet_index(*options.LastTabletIndex);
    }
}

void ToProto(
    NProto::TTabletReadOptions* protoOptions,
    const NApi::TTabletReadOptions& options)
{
    protoOptions->set_read_from(static_cast<NProto::ETabletReadKind>(options.ReadFrom));
}

////////////////////////////////////////////////////////////////////////////////
// CONFIGS
////////////////////////////////////////////////////////////////////////////////

void ToProto(
    NProto::TRetentionConfig* protoConfig,
    const NTableClient::TRetentionConfig& config)
{
    protoConfig->set_min_data_versions(config.MinDataVersions);
    protoConfig->set_max_data_versions(config.MaxDataVersions);
    protoConfig->set_min_data_ttl(config.MinDataTtl.GetValue());
    protoConfig->set_max_data_ttl(config.MaxDataTtl.GetValue());
    protoConfig->set_ignore_major_timestamp(config.IgnoreMajorTimestamp);
}

void FromProto(
    NTableClient::TRetentionConfig* config,
    const NProto::TRetentionConfig& protoConfig)
{
    config->MinDataVersions = protoConfig.min_data_versions();
    config->MaxDataVersions = protoConfig.max_data_versions();
    config->MinDataTtl = TDuration::FromValue(protoConfig.min_data_ttl());
    config->MaxDataTtl = TDuration::FromValue(protoConfig.max_data_ttl());
    config->IgnoreMajorTimestamp = protoConfig.ignore_major_timestamp();
}

////////////////////////////////////////////////////////////////////////////////
// RESULTS
////////////////////////////////////////////////////////////////////////////////

void ToProto(
    NProto::TGetFileFromCacheResult* proto,
    const NApi::TGetFileFromCacheResult& result)
{
    proto->set_path(result.Path);
}

void FromProto(
    NApi::TGetFileFromCacheResult* result,
    const NProto::TGetFileFromCacheResult& proto)
{
    result->Path = proto.path();
}

void ToProto(
    NProto::TPutFileToCacheResult* proto,
    const NApi::TPutFileToCacheResult& result)
{
    proto->set_path(result.Path);
}

void FromProto(
    NApi::TPutFileToCacheResult* result,
    const NProto::TPutFileToCacheResult& proto)
{
    result->Path = proto.path();
}

void ToProto(
    NProto::TCheckPermissionResult* proto,
    const NApi::TCheckPermissionResult& result)
{
    proto->Clear();

    proto->set_action(static_cast<NProto::ESecurityAction>(result.Action));

    ToProto(proto->mutable_object_id(), result.ObjectId);
    if (result.ObjectName) {
        proto->set_object_name(*result.ObjectName);
    }

    ToProto(proto->mutable_subject_id(), result.SubjectId);
    if (result.SubjectName) {
        proto->set_subject_name(*result.SubjectName);
    }
}

void FromProto(
    NApi::TCheckPermissionResult* result,
    const NProto::TCheckPermissionResult& proto)
{
    result->Action = static_cast<NSecurityClient::ESecurityAction>(proto.action());

    FromProto(&result->ObjectId, proto.object_id());
    if (proto.has_object_name()) {
        result->ObjectName = proto.object_name();
    } else {
        result->ObjectName.reset();
    }

    FromProto(&result->SubjectId, proto.subject_id());
    if (proto.has_subject_name()) {
        result->SubjectName = proto.subject_name();
    } else {
        result->SubjectName.reset();
    }
}

void ToProto(
    NProto::TCheckPermissionByAclResult* proto,
    const NApi::TCheckPermissionByAclResult& result)
{
    proto->Clear();

    proto->set_action(static_cast<NProto::ESecurityAction>(result.Action));

    ToProto(proto->mutable_subject_id(), result.SubjectId);
    if (result.SubjectName) {
        proto->set_subject_name(*result.SubjectName);
    }

    NYT::ToProto(proto->mutable_missing_subjects(), result.MissingSubjects);
}

void FromProto(
    NApi::TCheckPermissionByAclResult* result,
    const NProto::TCheckPermissionByAclResult& proto)
{
    result->Action = static_cast<NSecurityClient::ESecurityAction>(proto.action());

    FromProto(&result->SubjectId, proto.subject_id());
    if (proto.has_subject_name()) {
        result->SubjectName = proto.subject_name();
    } else {
        result->SubjectName.reset();
    }

    NYT::FromProto(&result->MissingSubjects, proto.missing_subjects());
}

void ToProto(
    NProto::TListOperationsResult* proto,
    const NApi::TListOperationsResult& result)
{
    proto->Clear();
    NYT::ToProto(proto->mutable_operations(), result.Operations);

    if (result.PoolCounts) {
        for (const auto& entry: *result.PoolCounts) {
            auto* newPoolCount = proto->mutable_pool_counts()->add_entries();
            newPoolCount->set_pool(entry.first);
            newPoolCount->set_count(entry.second);
        }
    }
    if (result.UserCounts) {
        for (const auto& entry: *result.UserCounts) {
            auto* newUserCount = proto->mutable_user_counts()->add_entries();
            newUserCount->set_user(entry.first);
            newUserCount->set_count(entry.second);
        }
    }

    if (result.StateCounts) {
        for (const auto& state: TEnumTraits<NScheduler::EOperationState>::GetDomainValues()) {
            if ((*result.StateCounts)[state] != 0) {
                auto* newStateCount = proto->mutable_state_counts()->add_entries();
                newStateCount->set_state(ConvertOperationStateToProto(state));
                newStateCount->set_count((*result.StateCounts)[state]);
            }
        }
    }
    if (result.TypeCounts) {
        for (const auto& type: TEnumTraits<NScheduler::EOperationType>::GetDomainValues()) {
            if ((*result.TypeCounts)[type] != 0) {
                auto* newTypeCount = proto->mutable_type_counts()->add_entries();
                newTypeCount->set_type(ConvertOperationTypeToProto(type));
                newTypeCount->set_count((*result.TypeCounts)[type]);
            }
        }
    }

    if (result.FailedJobsCount) {
        proto->set_failed_jobs_count(*result.FailedJobsCount);
    }
    proto->set_incomplete(result.Incomplete);
}

void FromProto(
    NApi::TListOperationsResult* result,
    const NProto::TListOperationsResult& proto)
{
    NYT::FromProto(&result->Operations, proto.operations());

    if (proto.has_pool_counts()) {
        result->PoolCounts.emplace();
        for (const auto& poolCount: proto.pool_counts().entries()) {
            auto pool = poolCount.pool();
            YT_VERIFY((*result->PoolCounts)[pool] == 0);
            (*result->PoolCounts)[pool] = poolCount.count();
        }
    } else {
        result->PoolCounts.reset();
    }
    if (proto.has_user_counts()) {
        result->UserCounts.emplace();
        for (const auto& userCount: proto.user_counts().entries()) {
            auto user = userCount.user();
            YT_VERIFY((*result->UserCounts)[user] == 0);
            (*result->UserCounts)[user] = userCount.count();
        }
    } else {
        result->UserCounts.reset();
    }

    if (proto.has_state_counts()) {
        result->StateCounts.emplace();
        std::fill(result->StateCounts->begin(), result->StateCounts->end(), 0);
        for (const auto& stateCount: proto.state_counts().entries()) {
            auto state = ConvertOperationStateFromProto(stateCount.state());
            YT_VERIFY(result->StateCounts->IsDomainValue(state));
            YT_VERIFY((*result->StateCounts)[state] == 0);
            (*result->StateCounts)[state] = stateCount.count();
        }
    } else {
        result->StateCounts.reset();
    }
    if (proto.has_type_counts()) {
        result->TypeCounts.emplace();
        std::fill(result->TypeCounts->begin(), result->TypeCounts->end(), 0);
        for (const auto& typeCount: proto.type_counts().entries()) {
            auto type = ConvertOperationTypeFromProto(typeCount.type());
            YT_VERIFY(result->TypeCounts->IsDomainValue(type));
            YT_VERIFY((*result->TypeCounts)[type] == 0);
            (*result->TypeCounts)[type] = typeCount.count();
        }
    } else {
        result->TypeCounts.reset();
    }

    if (proto.has_failed_jobs_count()) {
        result->FailedJobsCount = proto.failed_jobs_count();
    } else {
        result->FailedJobsCount.reset();
    }
    result->Incomplete = proto.incomplete();
}

void ToProto(
    NProto::TListJobsResult* proto,
    const NApi::TListJobsResult& result)
{
    proto->Clear();
    NYT::ToProto(proto->mutable_jobs(), result.Jobs);

    if (result.CypressJobCount) {
        proto->set_cypress_job_count(*result.CypressJobCount);
    }
    if (result.ControllerAgentJobCount) {
        proto->set_controller_agent_job_count(*result.ControllerAgentJobCount);
    }
    if (result.ArchiveJobCount) {
        proto->set_archive_job_count(*result.ArchiveJobCount);
    }

    ToProto(proto->mutable_statistics(), result.Statistics);
    NYT::ToProto(proto->mutable_errors(), result.Errors);
}

void FromProto(
    NApi::TListJobsResult* result,
    const NProto::TListJobsResult& proto)
{
    NYT::FromProto(&result->Jobs, proto.jobs());

    if (proto.has_cypress_job_count()) {
        result->CypressJobCount = proto.cypress_job_count();
    } else {
        result->CypressJobCount.reset();
    }
    if (proto.has_controller_agent_job_count()) {
        result->ControllerAgentJobCount = proto.controller_agent_job_count();
    } else {
        result->ControllerAgentJobCount.reset();
    }
    if (proto.has_archive_job_count()) {
        result->ArchiveJobCount = proto.archive_job_count();
    } else {
        result->ArchiveJobCount.reset();
    }

    FromProto(&result->Statistics, proto.statistics());
    NYT::FromProto(&result->Errors, proto.errors());
}

////////////////////////////////////////////////////////////////////////////////
// MISC
////////////////////////////////////////////////////////////////////////////////

void ToProto(NProto::TColumnSchema* protoSchema, const NTableClient::TColumnSchema& schema)
{
    protoSchema->set_name(schema.Name());
    protoSchema->set_type(static_cast<int>(schema.GetPhysicalType()));
    if (!schema.SimplifiedLogicalType()) {
        THROW_ERROR_EXCEPTION("Complex logical types are not supported in rpc yet")
            << TErrorAttribute("name", schema.Name())
            << TErrorAttribute("type", ToString(*schema.LogicalType()));
    }
    if (schema.Lock()) {
        protoSchema->set_lock(*schema.Lock());
    }
    if (schema.Expression()) {
        protoSchema->set_expression(*schema.Expression());
    }
    if (schema.Aggregate()) {
        protoSchema->set_aggregate(*schema.Aggregate());
    }
    if (schema.SortOrder()) {
        protoSchema->set_sort_order(static_cast<int>(*schema.SortOrder()));
    }
    if (schema.Group()) {
        protoSchema->set_group(*schema.Group());
    }
    if (schema.Required()) {
        protoSchema->set_required(schema.Required());
    }
}

void FromProto(NTableClient::TColumnSchema* schema, const NProto::TColumnSchema& protoSchema)
{
    schema->SetName(protoSchema.name());
    if (protoSchema.has_logical_type()) {
        auto logicalType = MakeLogicalType(
            CheckedEnumCast<NTableClient::ESimpleLogicalValueType>(protoSchema.logical_type()),
            protoSchema.required());
        schema->SetLogicalType(std::move(logicalType));
        YT_VERIFY(schema->GetPhysicalType() == CheckedEnumCast<EValueType>(protoSchema.type()));
    } else {
        auto physicalType = CheckedEnumCast<NTableClient::EValueType>(protoSchema.type());
        schema->SetLogicalType(MakeLogicalType(NTableClient::GetLogicalType(physicalType), protoSchema.required()));
    }
    schema->SetLock(protoSchema.has_lock() ? std::make_optional(protoSchema.lock()) : std::nullopt);
    schema->SetExpression(protoSchema.has_expression() ? std::make_optional(protoSchema.expression()) : std::nullopt);
    schema->SetAggregate(protoSchema.has_aggregate() ? std::make_optional(protoSchema.aggregate()) : std::nullopt);
    schema->SetSortOrder(protoSchema.has_sort_order() ? std::make_optional(ESortOrder(protoSchema.sort_order())) : std::nullopt);
    schema->SetGroup(protoSchema.has_group() ? std::make_optional(protoSchema.group()) : std::nullopt);
}

void ToProto(NProto::TTableSchema* protoSchema, const NTableClient::TTableSchema& schema)
{
    using NYT::ToProto;

    ToProto(protoSchema->mutable_columns(), schema.Columns());
    protoSchema->set_strict(schema.GetStrict());
    protoSchema->set_unique_keys(schema.GetUniqueKeys());
}

void FromProto(NTableClient::TTableSchema* schema, const NProto::TTableSchema& protoSchema)
{
    using NYT::FromProto;

    *schema = NTableClient::TTableSchema(
        FromProto<std::vector<NTableClient::TColumnSchema>>(protoSchema.columns()),
        protoSchema.strict(),
        protoSchema.unique_keys());
}

void ToProto(NProto::TTabletInfo* protoTabletInfo, const NTabletClient::TTabletInfo& tabletInfo)
{
    ToProto(protoTabletInfo->mutable_tablet_id(), tabletInfo.TabletId);
    protoTabletInfo->set_mount_revision(tabletInfo.MountRevision);
    protoTabletInfo->set_state(static_cast<i32>(tabletInfo.State));
    ToProto(protoTabletInfo->mutable_pivot_key(), tabletInfo.PivotKey);
    if (tabletInfo.CellId) {
        ToProto(protoTabletInfo->mutable_cell_id(), tabletInfo.CellId);
    }
}

void FromProto(NTabletClient::TTabletInfo* tabletInfo, const NProto::TTabletInfo& protoTabletInfo)
{
    using NYT::FromProto;

    tabletInfo->TabletId =
        FromProto<TTabletId>(protoTabletInfo.tablet_id());
    tabletInfo->MountRevision = protoTabletInfo.mount_revision();
    tabletInfo->State = CheckedEnumCast<ETabletState>(protoTabletInfo.state());
    tabletInfo->PivotKey = FromProto<NTableClient::TOwningKey>(protoTabletInfo.pivot_key());
    if (protoTabletInfo.has_cell_id()) {
        tabletInfo->CellId = FromProto<TTabletCellId>(protoTabletInfo.cell_id());
    }
}

void ToProto(
    NProto::TQueryStatistics* protoStatistics,
    const NQueryClient::TQueryStatistics& statistics)
{
    protoStatistics->set_rows_read(statistics.RowsRead);
    protoStatistics->set_data_weight_read(statistics.DataWeightRead);
    protoStatistics->set_rows_written(statistics.RowsWritten);
    protoStatistics->set_sync_time(statistics.SyncTime.GetValue());
    protoStatistics->set_async_time(statistics.AsyncTime.GetValue());
    protoStatistics->set_execute_time(statistics.ExecuteTime.GetValue());
    protoStatistics->set_read_time(statistics.ReadTime.GetValue());
    protoStatistics->set_write_time(statistics.WriteTime.GetValue());
    protoStatistics->set_codegen_time(statistics.CodegenTime.GetValue());
    protoStatistics->set_wait_on_ready_event_time(statistics.WaitOnReadyEventTime.GetValue());
    protoStatistics->set_incomplete_input(statistics.IncompleteInput);
    protoStatistics->set_incomplete_output(statistics.IncompleteOutput);
    protoStatistics->set_memory_usage(statistics.MemoryUsage);

    NYT::ToProto(protoStatistics->mutable_inner_statistics(), statistics.InnerStatistics);
}

void FromProto(
    NQueryClient::TQueryStatistics* statistics,
    const NProto::TQueryStatistics& protoStatistics)
{
    statistics->RowsRead = protoStatistics.rows_read();
    statistics->DataWeightRead = protoStatistics.data_weight_read();
    statistics->RowsWritten = protoStatistics.rows_written();
    statistics->SyncTime = TDuration::FromValue(protoStatistics.sync_time());
    statistics->AsyncTime = TDuration::FromValue(protoStatistics.async_time());
    statistics->ExecuteTime = TDuration::FromValue(protoStatistics.execute_time());
    statistics->ReadTime = TDuration::FromValue(protoStatistics.read_time());
    statistics->WriteTime = TDuration::FromValue(protoStatistics.write_time());
    statistics->CodegenTime = TDuration::FromValue(protoStatistics.codegen_time());
    statistics->WaitOnReadyEventTime = TDuration::FromValue(protoStatistics.wait_on_ready_event_time());
    statistics->IncompleteInput = protoStatistics.incomplete_input();
    statistics->IncompleteOutput = protoStatistics.incomplete_output();
    statistics->MemoryUsage = protoStatistics.memory_usage();

    NYT::FromProto(&statistics->InnerStatistics, protoStatistics.inner_statistics());
}

void ToProto(NProto::TOperation* protoOperation, const NApi::TOperation& operation)
{
    protoOperation->Clear();

    if (operation.Id) {
        ToProto(protoOperation->mutable_id(), *operation.Id);
    }
    if (operation.Type) {
        protoOperation->set_type(ConvertOperationTypeToProto(*operation.Type));
    }
    if (operation.State) {
        protoOperation->set_state(ConvertOperationStateToProto(*operation.State));
    }

    if (operation.StartTime) {
        protoOperation->set_start_time(NYT::ToProto<i64>(*operation.StartTime));
    }
    if (operation.FinishTime) {
        protoOperation->set_finish_time(NYT::ToProto<i64>(*operation.FinishTime));
    }

    if (operation.AuthenticatedUser) {
        protoOperation->set_authenticated_user(*operation.AuthenticatedUser);
    }

    if (operation.BriefSpec) {
        protoOperation->set_brief_spec(operation.BriefSpec.GetData());
    }
    if (operation.Spec) {
        protoOperation->set_spec(operation.Spec.GetData());
    }
    if (operation.FullSpec) {
        protoOperation->set_full_spec(operation.FullSpec.GetData());
    }
    if (operation.UnrecognizedSpec) {
        protoOperation->set_unrecognized_spec(operation.UnrecognizedSpec.GetData());
    }

    if (operation.BriefProgress) {
        protoOperation->set_brief_progress(operation.BriefProgress.GetData());
    }
    if (operation.Progress) {
        protoOperation->set_progress(operation.Progress.GetData());
    }

    if (operation.RuntimeParameters) {
        protoOperation->set_runtime_parameters(operation.RuntimeParameters.GetData());
    }

    if (operation.Suspended) {
        protoOperation->set_suspended(*operation.Suspended);
    }

    if (operation.Events) {
        protoOperation->set_events(operation.Events.GetData());
    }
    if (operation.Result) {
        protoOperation->set_result(operation.Result.GetData());
    }

    if (operation.SlotIndexPerPoolTree) {
        protoOperation->set_slot_index_per_pool_tree(operation.SlotIndexPerPoolTree.GetData());
    }
}

void FromProto(NApi::TOperation* operation, const NProto::TOperation& protoOperation)
{
    if (protoOperation.has_id()) {
        operation->Id = NYT::FromProto<NScheduler::TOperationId>(protoOperation.id());
    } else {
        operation->Id.reset();
    }
    if (protoOperation.has_type()) {
        operation->Type = ConvertOperationTypeFromProto(protoOperation.type());
    } else {
        operation->Type.reset();
    }
    if (protoOperation.has_state()) {
        operation->State = ConvertOperationStateFromProto(protoOperation.state());
    } else {
        operation->State.reset();
    }

    if (protoOperation.has_start_time()) {
        operation->StartTime = TInstant::FromValue(protoOperation.start_time());
    } else {
        operation->StartTime.reset();
    }
    if (protoOperation.has_finish_time()) {
        operation->FinishTime = TInstant::FromValue(protoOperation.finish_time());
    } else {
        operation->FinishTime.reset();
    }

    if (protoOperation.has_authenticated_user()) {
        operation->AuthenticatedUser = protoOperation.authenticated_user();
    } else {
        operation->AuthenticatedUser.reset();
    }

    if (protoOperation.has_brief_spec()) {
        operation->BriefSpec = NYson::TYsonString(protoOperation.brief_spec());
    } else {
        operation->BriefSpec = NYson::TYsonString();
    }
    if (protoOperation.has_spec()) {
        operation->Spec = NYson::TYsonString(protoOperation.spec());
    } else {
        operation->Spec = NYson::TYsonString();
    }
    if (protoOperation.has_full_spec()) {
        operation->FullSpec = NYson::TYsonString(protoOperation.full_spec());
    } else {
        operation->FullSpec = NYson::TYsonString();
    }
    if (protoOperation.has_unrecognized_spec()) {
        operation->UnrecognizedSpec = NYson::TYsonString(protoOperation.unrecognized_spec());
    } else {
        operation->UnrecognizedSpec = NYson::TYsonString();
    }

    if (protoOperation.has_brief_progress()) {
        operation->BriefProgress = NYson::TYsonString(protoOperation.brief_progress());
    } else {
        operation->BriefProgress = NYson::TYsonString();
    }
    if (protoOperation.has_progress()) {
        operation->Progress = NYson::TYsonString(protoOperation.progress());
    } else {
        operation->Progress = NYson::TYsonString();
    }

    if (protoOperation.has_runtime_parameters()) {
        operation->RuntimeParameters = NYson::TYsonString(protoOperation.runtime_parameters());
    } else {
        operation->RuntimeParameters = NYson::TYsonString();
    }

    if (protoOperation.has_suspended()) {
        operation->Suspended = protoOperation.suspended();
    } else {
        operation->Suspended.reset();
    }

    if (protoOperation.has_events()) {
        operation->Events = NYson::TYsonString(protoOperation.events());
    } else {
        operation->Events = NYson::TYsonString();
    }
    if (protoOperation.has_result()) {
        operation->Result = NYson::TYsonString(protoOperation.result());
    } else {
        operation->Result = NYson::TYsonString();
    }

    if (protoOperation.has_slot_index_per_pool_tree()) {
        operation->SlotIndexPerPoolTree = NYson::TYsonString(protoOperation.slot_index_per_pool_tree());
    } else {
        operation->SlotIndexPerPoolTree = NYson::TYsonString();
    }
}

void ToProto(NProto::TJob* protoJob, const NApi::TJob& job)
{
    protoJob->Clear();

    if (job.Id) {
        ToProto(protoJob->mutable_id(), job.Id);
    }
    if (job.OperationId) {
        ToProto(protoJob->mutable_operation_id(), job.OperationId);
    }
    if (job.Type) {
        protoJob->set_type(ConvertJobTypeToProto(*job.Type));
    }
    if (job.State) {
        protoJob->set_state(ConvertJobStateToProto(*job.State));
    }
    if (job.ControllerAgentState) {
        protoJob->set_controller_agent_state(ConvertJobStateToProto(*job.ControllerAgentState));
    }
    if (job.ArchiveState) {
        protoJob->set_archive_state(ConvertJobStateToProto(*job.ArchiveState));
    }

    if (job.StartTime) {
        protoJob->set_start_time(NYT::ToProto<i64>(*job.StartTime));
    }
    if (job.FinishTime) {
        protoJob->set_finish_time(NYT::ToProto<i64>(*job.FinishTime));
    }

    if (job.Address) {
        protoJob->set_address(*job.Address);
    }
    if (job.Progress) {
        protoJob->set_progress(*job.Progress);
    }
    if (job.StderrSize) {
        protoJob->set_stderr_size(*job.StderrSize);
    }
    if (job.FailContextSize) {
        protoJob->set_fail_context_size(*job.FailContextSize);
    }
    if (job.HasSpec) {
        protoJob->set_has_spec(*job.HasSpec);
    }

    if (job.Error) {
        protoJob->set_error(job.Error.GetData());
    }
    if (job.BriefStatistics) {
        protoJob->set_brief_statistics(job.BriefStatistics.GetData());
    }
    if (job.InputPaths) {
        protoJob->set_input_paths(job.InputPaths.GetData());
    }
    if (job.CoreInfos) {
        protoJob->set_core_infos(job.CoreInfos.GetData());
    }
    if (job.JobCompetitionId) {
        ToProto(protoJob->mutable_job_competition_id(), job.JobCompetitionId);
    }
    if (job.HasCompetitors) {
        protoJob->set_has_competitors(*job.HasCompetitors);
    }
    if (job.IsStale) {
        protoJob->set_is_stale(*job.IsStale);
    }
    if (job.ExecAttributes) {
        protoJob->set_exec_attributes(job.ExecAttributes.GetData());
    }
}

void FromProto(NApi::TJob* job, const NProto::TJob& protoJob)
{
    if (protoJob.has_id()) {
        FromProto(&job->Id, protoJob.id());
    } else {
        job->Id = {};
    }
    if (protoJob.has_operation_id()) {
        FromProto(&job->OperationId, protoJob.operation_id());
    } else {
        job->OperationId = {};
    }
    if (protoJob.has_type()) {
        job->Type = ConvertJobTypeFromProto(protoJob.type());
    } else {
        job->Type.reset();
    }
    if (protoJob.has_state()) {
        job->State = ConvertJobStateFromProto(protoJob.state());
    } else {
        job->State.reset();
    }
    if (protoJob.has_controller_agent_state()) {
        job->ControllerAgentState = ConvertJobStateFromProto(protoJob.controller_agent_state());
    } else {
        job->ControllerAgentState.reset();
    }
    if (protoJob.has_archive_state()) {
        job->ArchiveState = ConvertJobStateFromProto(protoJob.archive_state());
    } else {
        job->ArchiveState.reset();
    }
    if (protoJob.has_start_time()) {
        job->StartTime = TInstant::FromValue(protoJob.start_time());
    } else {
        job->StartTime.reset();
    }
    if (protoJob.has_finish_time()) {
        job->FinishTime = TInstant::FromValue(protoJob.finish_time());
    } else {
        job->FinishTime.reset();
    }
    if (protoJob.has_address()) {
        job->Address = protoJob.address();
    } else {
        job->Address.reset();
    }
    if (protoJob.has_progress()) {
        job->Progress = protoJob.progress();
    } else {
        job->Progress.reset();
    }
    if (protoJob.has_stderr_size()) {
        job->StderrSize = protoJob.stderr_size();
    } else {
        job->StderrSize.reset();
    }
    if (protoJob.has_fail_context_size()) {
        job->FailContextSize = protoJob.fail_context_size();
    } else {
        job->FailContextSize.reset();
    }
    if (protoJob.has_has_spec()) {
        job->HasSpec = protoJob.has_spec();
    } else {
        job->HasSpec = false;
    }
    if (protoJob.has_error()) {
        job->Error = NYson::TYsonString(protoJob.error());
    } else {
        job->Error = NYson::TYsonString();
    }
    if (protoJob.has_brief_statistics()) {
        job->BriefStatistics = NYson::TYsonString(protoJob.brief_statistics());
    } else {
        job->BriefStatistics = NYson::TYsonString();
    }
    if (protoJob.has_input_paths()) {
        job->InputPaths = NYson::TYsonString(protoJob.input_paths());
    } else {
        job->InputPaths = NYson::TYsonString();
    }
    if (protoJob.has_core_infos()) {
        job->CoreInfos = NYson::TYsonString(protoJob.core_infos());
    } else {
        job->CoreInfos = NYson::TYsonString();
    }
    if (protoJob.has_job_competition_id()) {
        FromProto(&job->JobCompetitionId, protoJob.job_competition_id());
    } else {
        job->JobCompetitionId = {};
    }
    if (protoJob.has_has_competitors()) {
        job->HasCompetitors = protoJob.has_competitors();
    } else {
        job->HasCompetitors = false;
    }
    if (protoJob.has_is_stale()) {
        job->IsStale = protoJob.is_stale();
    } else {
        job->IsStale.reset();
    }
    if (protoJob.has_exec_attributes()) {
        job->ExecAttributes = NYson::TYsonString(protoJob.exec_attributes());
    } else {
        job->ExecAttributes = NYson::TYsonString();
    }
}

void ToProto(
    NProto::TListJobsStatistics* protoStatistics,
    const NApi::TListJobsStatistics& statistics)
{
    protoStatistics->mutable_state_counts()->clear_entries();
    for (const auto& state: TEnumTraits<NJobTrackerClient::EJobState>::GetDomainValues()) {
        if (statistics.StateCounts[state] != 0) {
            auto* newStateCount = protoStatistics->mutable_state_counts()->add_entries();
            newStateCount->set_state(ConvertJobStateToProto(state));
            newStateCount->set_count(statistics.StateCounts[state]);
        }
    }

    protoStatistics->mutable_type_counts()->clear_entries();
    for (const auto& type: TEnumTraits<NJobTrackerClient::EJobType>::GetDomainValues()) {
        if (statistics.TypeCounts[type] != 0) {
            auto* newTypeCount = protoStatistics->mutable_type_counts()->add_entries();
            newTypeCount->set_type(ConvertJobTypeToProto(type));
            newTypeCount->set_count(statistics.TypeCounts[type]);
        }
    }
}

void FromProto(
    NApi::TListJobsStatistics* statistics,
    const NProto::TListJobsStatistics& protoStatistics)
{
    std::fill(statistics->StateCounts.begin(), statistics->StateCounts.end(), 0);
    for (const auto& stateCount: protoStatistics.state_counts().entries()) {
        auto state = ConvertJobStateFromProto(stateCount.state());
        YT_VERIFY(statistics->StateCounts.IsDomainValue(state));
        YT_VERIFY(statistics->StateCounts[state] == 0);
        statistics->StateCounts[state] = stateCount.count();
    }

    std::fill(statistics->TypeCounts.begin(), statistics->TypeCounts.end(), 0);
    for (const auto& typeCount: protoStatistics.type_counts().entries()) {
        auto type = ConvertJobTypeFromProto(typeCount.type());
        YT_VERIFY(statistics->TypeCounts.IsDomainValue(type));
        YT_VERIFY(statistics->TypeCounts[type] == 0);
        statistics->TypeCounts[type] = typeCount.count();
    }
}

void ToProto(
    NProto::TColumnarStatistics* protoStatistics,
    const NTableClient::TColumnarStatistics& statistics)
{
    protoStatistics->Clear();

    NYT::ToProto(protoStatistics->mutable_column_data_weights(), statistics.ColumnDataWeights);
    if (statistics.TimestampTotalWeight) {
        protoStatistics->set_timestamp_total_weight(*statistics.TimestampTotalWeight);
    }
    protoStatistics->set_legacy_chunk_data_weight(statistics.LegacyChunkDataWeight);
}

void FromProto(
    NTableClient::TColumnarStatistics* statistics,
    const NProto::TColumnarStatistics& protoStatistics)
{
    NYT::FromProto(&statistics->ColumnDataWeights, protoStatistics.column_data_weights());
    if (protoStatistics.has_timestamp_total_weight()) {
        statistics->TimestampTotalWeight = protoStatistics.timestamp_total_weight();
    } else {
        statistics->TimestampTotalWeight.reset();
    }
    statistics->LegacyChunkDataWeight = protoStatistics.legacy_chunk_data_weight();
}

template <class TStringContainer>
void ToProto(
    NRpcProxy::NProto::TAttributeKeys* protoAttributes,
    const std::optional<TStringContainer>& attributes)
{
    if (attributes) {
        protoAttributes->set_all(false);
        NYT::ToProto(protoAttributes->mutable_columns(), *attributes);
    } else {
        protoAttributes->set_all(true);
    }
}

// Instantiate templates.
template void ToProto(
    NRpcProxy::NProto::TAttributeKeys* protoAttributes,
    const std::optional<std::vector<TString>>& attributes);
template void ToProto(
    NRpcProxy::NProto::TAttributeKeys* protoAttributes,
    const std::optional<THashSet<TString>>& attributes);

////////////////////////////////////////////////////////////////////////////////
// ENUMS
////////////////////////////////////////////////////////////////////////////////

NProto::EOperationType ConvertOperationTypeToProto(
    const NScheduler::EOperationType& operationType)
{
    switch (operationType) {
        case NScheduler::EOperationType::Map:
            return NProto::EOperationType::OT_MAP;
        case NScheduler::EOperationType::Merge:
            return NProto::EOperationType::OT_MERGE;
        case NScheduler::EOperationType::Erase:
            return NProto::EOperationType::OT_ERASE;
        case NScheduler::EOperationType::Sort:
            return NProto::EOperationType::OT_SORT;
        case NScheduler::EOperationType::Reduce:
            return NProto::EOperationType::OT_REDUCE;
        case NScheduler::EOperationType::MapReduce:
            return NProto::EOperationType::OT_MAP_REDUCE;
        case NScheduler::EOperationType::RemoteCopy:
            return NProto::EOperationType::OT_REMOTE_COPY;
        case NScheduler::EOperationType::JoinReduce:
            return NProto::EOperationType::OT_JOIN_REDUCE;
        case NScheduler::EOperationType::Vanilla:
            return NProto::EOperationType::OT_VANILLA;
        default:
            YT_ABORT();
    }
}

NScheduler::EOperationType ConvertOperationTypeFromProto(
    const NProto::EOperationType& proto)
{
    switch (proto) {
        case NProto::EOperationType::OT_MAP:
            return NScheduler::EOperationType::Map;
        case NProto::EOperationType::OT_MERGE:
            return NScheduler::EOperationType::Merge;
        case NProto::EOperationType::OT_ERASE:
            return NScheduler::EOperationType::Erase;
        case NProto::EOperationType::OT_SORT:
            return NScheduler::EOperationType::Sort;
        case NProto::EOperationType::OT_REDUCE:
            return NScheduler::EOperationType::Reduce;
        case NProto::EOperationType::OT_MAP_REDUCE:
            return NScheduler::EOperationType::MapReduce;
        case NProto::EOperationType::OT_REMOTE_COPY:
            return NScheduler::EOperationType::RemoteCopy;
        case NProto::EOperationType::OT_JOIN_REDUCE:
            return NScheduler::EOperationType::JoinReduce;
        case NProto::EOperationType::OT_VANILLA:
            return NScheduler::EOperationType::Vanilla;
        default:
            YT_ABORT();
    }
}

NProto::EOperationState ConvertOperationStateToProto(
    const NScheduler::EOperationState& operationState)
{
    switch (operationState) {
        case NScheduler::EOperationState::None:
            return NProto::EOperationState::OS_NONE;
        case NScheduler::EOperationState::Starting:
            return NProto::EOperationState::OS_STARTING;
        case NScheduler::EOperationState::Orphaned:
            return NProto::EOperationState::OS_ORPHANED;
        case NScheduler::EOperationState::WaitingForAgent:
            return NProto::EOperationState::OS_WAITING_FOR_AGENT;
        case NScheduler::EOperationState::Initializing:
            return NProto::EOperationState::OS_INITIALIZING;
        case NScheduler::EOperationState::Preparing:
            return NProto::EOperationState::OS_PREPARING;
        case NScheduler::EOperationState::Materializing:
            return NProto::EOperationState::OS_MATERIALIZING;
        case NScheduler::EOperationState::Reviving:
            return NProto::EOperationState::OS_REVIVING;
        case NScheduler::EOperationState::RevivingJobs:
            return NProto::EOperationState::OS_REVIVING_JOBS;
        case NScheduler::EOperationState::Pending:
            return NProto::EOperationState::OS_PENDING;
        case NScheduler::EOperationState::Running:
            return NProto::EOperationState::OS_RUNNING;
        case NScheduler::EOperationState::Completing:
            return NProto::EOperationState::OS_COMPLETING;
        case NScheduler::EOperationState::Completed:
            return NProto::EOperationState::OS_COMPLETED;
        case NScheduler::EOperationState::Aborting:
            return NProto::EOperationState::OS_ABORTING;
        case NScheduler::EOperationState::Aborted:
            return NProto::EOperationState::OS_ABORTED;
        case NScheduler::EOperationState::Failing:
            return NProto::EOperationState::OS_FAILING;
        case NScheduler::EOperationState::Failed:
            return NProto::EOperationState::OS_FAILED;
        default:
            YT_ABORT();
    }
}

NScheduler::EOperationState ConvertOperationStateFromProto(
    const NProto::EOperationState& proto)
{
    switch (proto) {
        case NProto::EOperationState::OS_NONE:
            return NScheduler::EOperationState::None;
        case NProto::EOperationState::OS_STARTING:
            return NScheduler::EOperationState::Starting;
        case NProto::EOperationState::OS_ORPHANED:
            return NScheduler::EOperationState::Orphaned;
        case NProto::EOperationState::OS_WAITING_FOR_AGENT:
            return NScheduler::EOperationState::WaitingForAgent;
        case NProto::EOperationState::OS_INITIALIZING:
            return NScheduler::EOperationState::Initializing;
        case NProto::EOperationState::OS_PREPARING:
            return NScheduler::EOperationState::Preparing;
        case NProto::EOperationState::OS_MATERIALIZING:
            return NScheduler::EOperationState::Materializing;
        case NProto::EOperationState::OS_REVIVING:
            return NScheduler::EOperationState::Reviving;
        case NProto::EOperationState::OS_REVIVING_JOBS:
            return NScheduler::EOperationState::RevivingJobs;
        case NProto::EOperationState::OS_PENDING:
            return NScheduler::EOperationState::Pending;
        case NProto::EOperationState::OS_RUNNING:
            return NScheduler::EOperationState::Running;
        case NProto::EOperationState::OS_COMPLETING:
            return NScheduler::EOperationState::Completing;
        case NProto::EOperationState::OS_COMPLETED:
            return NScheduler::EOperationState::Completed;
        case NProto::EOperationState::OS_ABORTING:
            return NScheduler::EOperationState::Aborting;
        case NProto::EOperationState::OS_ABORTED:
            return NScheduler::EOperationState::Aborted;
        case NProto::EOperationState::OS_FAILING:
            return NScheduler::EOperationState::Failing;
        case NProto::EOperationState::OS_FAILED:
            return NScheduler::EOperationState::Failed;
        default:
            YT_ABORT();
    }
}

NProto::EJobType ConvertJobTypeToProto(
    const NJobTrackerClient::EJobType& jobType)
{
    switch (jobType) {
        case NJobTrackerClient::EJobType::Map:
            return NProto::EJobType::JT_MAP;
        case NJobTrackerClient::EJobType::PartitionMap:
            return NProto::EJobType::JT_PARTITION_MAP;
        case NJobTrackerClient::EJobType::SortedMerge:
            return NProto::EJobType::JT_SORTED_MERGE;
        case NJobTrackerClient::EJobType::OrderedMerge:
            return NProto::EJobType::JT_ORDERED_MERGE;
        case NJobTrackerClient::EJobType::UnorderedMerge:
            return NProto::EJobType::JT_UNORDERED_MERGE;
        case NJobTrackerClient::EJobType::Partition:
            return NProto::EJobType::JT_PARTITION;
        case NJobTrackerClient::EJobType::SimpleSort:
            return NProto::EJobType::JT_SIMPLE_SORT;
        case NJobTrackerClient::EJobType::FinalSort:
            return NProto::EJobType::JT_FINAL_SORT;
        case NJobTrackerClient::EJobType::SortedReduce:
            return NProto::EJobType::JT_SORTED_REDUCE;
        case NJobTrackerClient::EJobType::PartitionReduce:
            return NProto::EJobType::JT_PARTITION_REDUCE;
        case NJobTrackerClient::EJobType::ReduceCombiner:
            return NProto::EJobType::JT_REDUCE_COMBINER;
        case NJobTrackerClient::EJobType::RemoteCopy:
            return NProto::EJobType::JT_REMOTE_COPY;
        case NJobTrackerClient::EJobType::IntermediateSort:
            return NProto::EJobType::JT_INTERMEDIATE_SORT;
        case NJobTrackerClient::EJobType::OrderedMap:
            return NProto::EJobType::JT_ORDERED_MAP;
        case NJobTrackerClient::EJobType::JoinReduce:
            return NProto::EJobType::JT_JOIN_REDUCE;
        case NJobTrackerClient::EJobType::Vanilla:
            return NProto::EJobType::JT_VANILLA;
        case NJobTrackerClient::EJobType::SchedulerUnknown:
            return NProto::EJobType::JT_SCHEDULER_UNKNOWN;
        case NJobTrackerClient::EJobType::ReplicateChunk:
            return NProto::EJobType::JT_REPLICATE_CHUNK;
        case NJobTrackerClient::EJobType::RemoveChunk:
            return NProto::EJobType::JT_REMOVE_CHUNK;
        case NJobTrackerClient::EJobType::RepairChunk:
            return NProto::EJobType::JT_REPAIR_CHUNK;
        case NJobTrackerClient::EJobType::SealChunk:
            return NProto::EJobType::JT_SEAL_CHUNK;
        default:
            YT_ABORT();
    }
}

NJobTrackerClient::EJobType ConvertJobTypeFromProto(
    const NProto::EJobType& proto)
{
    switch (proto) {
        case NProto::EJobType::JT_MAP:
            return NJobTrackerClient::EJobType::Map;
        case NProto::EJobType::JT_PARTITION_MAP:
            return NJobTrackerClient::EJobType::PartitionMap;
        case NProto::EJobType::JT_SORTED_MERGE:
            return NJobTrackerClient::EJobType::SortedMerge;
        case NProto::EJobType::JT_ORDERED_MERGE:
            return NJobTrackerClient::EJobType::OrderedMerge;
        case NProto::EJobType::JT_UNORDERED_MERGE:
            return NJobTrackerClient::EJobType::UnorderedMerge;
        case NProto::EJobType::JT_PARTITION:
            return NJobTrackerClient::EJobType::Partition;
        case NProto::EJobType::JT_SIMPLE_SORT:
            return NJobTrackerClient::EJobType::SimpleSort;
        case NProto::EJobType::JT_FINAL_SORT:
            return NJobTrackerClient::EJobType::FinalSort;
        case NProto::EJobType::JT_SORTED_REDUCE:
            return NJobTrackerClient::EJobType::SortedReduce;
        case NProto::EJobType::JT_PARTITION_REDUCE:
            return NJobTrackerClient::EJobType::PartitionReduce;
        case NProto::EJobType::JT_REDUCE_COMBINER:
            return NJobTrackerClient::EJobType::ReduceCombiner;
        case NProto::EJobType::JT_REMOTE_COPY:
            return NJobTrackerClient::EJobType::RemoteCopy;
        case NProto::EJobType::JT_INTERMEDIATE_SORT:
            return NJobTrackerClient::EJobType::IntermediateSort;
        case NProto::EJobType::JT_ORDERED_MAP:
            return NJobTrackerClient::EJobType::OrderedMap;
        case NProto::EJobType::JT_JOIN_REDUCE:
            return NJobTrackerClient::EJobType::JoinReduce;
        case NProto::EJobType::JT_VANILLA:
            return NJobTrackerClient::EJobType::Vanilla;
        case NProto::EJobType::JT_SCHEDULER_UNKNOWN:
            return NJobTrackerClient::EJobType::SchedulerUnknown;
        case NProto::EJobType::JT_REPLICATE_CHUNK:
            return NJobTrackerClient::EJobType::ReplicateChunk;
        case NProto::EJobType::JT_REMOVE_CHUNK:
            return NJobTrackerClient::EJobType::RemoveChunk;
        case NProto::EJobType::JT_REPAIR_CHUNK:
            return NJobTrackerClient::EJobType::RepairChunk;
        case NProto::EJobType::JT_SEAL_CHUNK:
            return NJobTrackerClient::EJobType::SealChunk;
        default:
            YT_ABORT();
    }
}

NProto::EJobState ConvertJobStateToProto(
    const NJobTrackerClient::EJobState& jobState)
{
    switch (jobState) {
        case NJobTrackerClient::EJobState::Waiting:
            return NProto::EJobState::JS_WAITING;
        case NJobTrackerClient::EJobState::Running:
            return NProto::EJobState::JS_RUNNING;
        case NJobTrackerClient::EJobState::Aborting:
            return NProto::EJobState::JS_ABORTING;
        case NJobTrackerClient::EJobState::Completed:
            return NProto::EJobState::JS_COMPLETED;
        case NJobTrackerClient::EJobState::Failed:
            return NProto::EJobState::JS_FAILED;
        case NJobTrackerClient::EJobState::Aborted:
            return NProto::EJobState::JS_ABORTED;
        case NJobTrackerClient::EJobState::Lost:
            return NProto::EJobState::JS_LOST;
        case NJobTrackerClient::EJobState::None:
            return NProto::EJobState::JS_NONE;
        default:
            YT_ABORT();
    }
}

NJobTrackerClient::EJobState ConvertJobStateFromProto(
    const NProto::EJobState& proto)
{
    switch (proto) {
        case NProto::EJobState::JS_WAITING:
            return NJobTrackerClient::EJobState::Waiting;
        case NProto::EJobState::JS_RUNNING:
            return NJobTrackerClient::EJobState::Running;
        case NProto::EJobState::JS_ABORTING:
            return NJobTrackerClient::EJobState::Aborting;
        case NProto::EJobState::JS_COMPLETED:
            return NJobTrackerClient::EJobState::Completed;
        case NProto::EJobState::JS_FAILED:
            return NJobTrackerClient::EJobState::Failed;
        case NProto::EJobState::JS_ABORTED:
            return NJobTrackerClient::EJobState::Aborted;
        case NProto::EJobState::JS_LOST:
            return NJobTrackerClient::EJobState::Lost;
        case NProto::EJobState::JS_NONE:
            return NJobTrackerClient::EJobState::None;
        default:
            YT_ABORT();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NProto

////////////////////////////////////////////////////////////////////////////////

bool IsRetriableError(const TError& error, bool retryProxyBanned)
{
    if (error.FindMatching(NRpcProxy::EErrorCode::ProxyBanned)) {
        return retryProxyBanned;
    }

    //! Retriable error codes are based on the ones used in http client.
    return
        error.FindMatching(NRpc::EErrorCode::RequestQueueSizeLimitExceeded) ||
        error.FindMatching(NRpc::EErrorCode::TransportError) ||
        error.FindMatching(NRpc::EErrorCode::Unavailable) ||
        error.FindMatching(NSecurityClient::EErrorCode::RequestQueueSizeLimitExceeded);
}

////////////////////////////////////////////////////////////////////////////////

void SetTimeoutOptions(
    NRpc::TClientRequest& request,
    const TTimeoutOptions& options)
{
    request.SetTimeout(options.Timeout);
}

////////////////////////////////////////////////////////////////////////////////
// ROWSETS
////////////////////////////////////////////////////////////////////////////////

template <class TRow>
struct TRowsetTraits;

template <>
struct TRowsetTraits<TUnversionedRow>
{
    static constexpr NProto::ERowsetKind Kind = NProto::RK_UNVERSIONED;
};

template <>
struct TRowsetTraits<TVersionedRow>
{
    static constexpr NProto::ERowsetKind Kind = NProto::RK_VERSIONED;
};

struct TRpcProxyRowsetBufferTag
{ };

void ValidateRowsetDescriptor(
    const NProto::TRowsetDescriptor& descriptor,
    int expectedVersion,
    NProto::ERowsetKind expectedKind)
{
    if (descriptor.wire_format_version() != expectedVersion) {
        THROW_ERROR_EXCEPTION(
            "Incompatible rowset wire format version: expected %v, got %v",
            expectedVersion,
            descriptor.wire_format_version());
    }
    if (descriptor.rowset_kind() != expectedKind) {
        THROW_ERROR_EXCEPTION(
            "Incompatible rowset kind: expected %v, got %v",
            NProto::ERowsetKind_Name(expectedKind),
            NProto::ERowsetKind_Name(descriptor.rowset_kind()));
    }
}

std::vector<TSharedRef> SerializeRowsetWithPartialNameTable(
    const NTableClient::TNameTablePtr& nameTable,
    int startingId,
    TRange<NTableClient::TUnversionedRow> rows,
    NProto::TRowsetDescriptor* descriptor)
{
    descriptor->Clear();
    descriptor->set_wire_format_version(NApi::NRpcProxy::CurrentWireFormatVersion);
    descriptor->set_rowset_kind(NProto::RK_UNVERSIONED);
    if (startingId < 0 || startingId > nameTable->GetSize()) {
        THROW_ERROR_EXCEPTION("Invalid starting id: expected in range [0, %v], got %v",
            nameTable->GetSize(),
            startingId);
    }
    for (int id = startingId; id < nameTable->GetSize(); ++id) {
        auto* columnDescriptor = descriptor->add_columns();
        columnDescriptor->set_name(TString(nameTable->GetName(id)));
    }
    TWireProtocolWriter writer;
    writer.WriteUnversionedRowset(rows);
    return writer.Finish();
}

std::vector<TSharedRef> SerializeRowset(
    const NTableClient::TNameTablePtr& nameTable,
    TRange<NTableClient::TUnversionedRow> rows,
    NProto::TRowsetDescriptor* descriptor)
{
    return SerializeRowsetWithPartialNameTable(nameTable, 0, rows, descriptor);
}

template <class TRow>
std::vector<TSharedRef> SerializeRowset(
    const TTableSchema& schema,
    TRange<TRow> rows,
    NProto::TRowsetDescriptor* descriptor)
{
    descriptor->Clear();
    descriptor->set_wire_format_version(NApi::NRpcProxy::CurrentWireFormatVersion);
    descriptor->set_rowset_kind(TRowsetTraits<TRow>::Kind);
    for (const auto& column : schema.Columns()) {
        auto* columnDescriptor = descriptor->add_columns();
        columnDescriptor->set_name(column.Name());
        // we save physical type for backward compatibility
        columnDescriptor->set_type(static_cast<int>(column.GetPhysicalType()));
        // TODO (ermolovd) YT-7178, support complex schemas.
        if (!column.SimplifiedLogicalType()) {
            THROW_ERROR_EXCEPTION("Serialization of complex types is not supported yet")
                << TErrorAttribute("column_name", column.Name())
                << TErrorAttribute("type", ToString(*column.LogicalType()));
        }
        columnDescriptor->set_logical_type(static_cast<int>(*column.SimplifiedLogicalType()));
    }
    TWireProtocolWriter writer;
    writer.WriteRowset(rows);
    return writer.Finish();
}

// Instantiate templates.
template std::vector<TSharedRef> SerializeRowset(
    const TTableSchema& schema,
    TRange<TUnversionedRow> rows,
    NProto::TRowsetDescriptor* descriptor);
template std::vector<TSharedRef> SerializeRowset(
    const TTableSchema& schema,
    TRange<TVersionedRow> rows,
    NProto::TRowsetDescriptor* descriptor);

TTableSchema DeserializeRowsetSchema(
    const NProto::TRowsetDescriptor& descriptor)
{
    std::vector<TColumnSchema> columns;
    columns.resize(descriptor.columns_size());
    for (int i = 0; i < descriptor.columns_size(); ++i) {
        if (descriptor.columns(i).has_name()) {
            columns[i].SetName(descriptor.columns(i).name());
        }
        // TODO (ermolovd) YT-7178, support complex schemas.
        if (descriptor.columns(i).has_logical_type()) {
            auto simpleLogicalType = CheckedEnumCast<NTableClient::ESimpleLogicalValueType>(descriptor.columns(i).logical_type());
            columns[i].SetLogicalType(OptionalLogicalType(SimpleLogicalType(simpleLogicalType)));
        } else if (descriptor.columns(i).has_type()) {
            auto simpleLogicalType = CheckedEnumCast<NTableClient::ESimpleLogicalValueType>(descriptor.columns(i).type());
            columns[i].SetLogicalType(OptionalLogicalType(SimpleLogicalType(simpleLogicalType)));
        }
    }
    return TTableSchema(std::move(columns));
}

template <class TRow>
TIntrusivePtr<NApi::IRowset<TRow>> DeserializeRowset(
    const NProto::TRowsetDescriptor& descriptor,
    const TSharedRef& data)
{
    ValidateRowsetDescriptor(
        descriptor,
        NApi::NRpcProxy::CurrentWireFormatVersion,
        TRowsetTraits<TRow>::Kind);
    TWireProtocolReader reader(data, New<TRowBuffer>(TRpcProxyRowsetBufferTag()));
    auto schema = DeserializeRowsetSchema(descriptor);
    auto schemaData = TWireProtocolReader::GetSchemaData(schema, TColumnFilter());
    auto rows = reader.ReadRowset<TRow>(schemaData, true);
    return NApi::CreateRowset(std::move(schema), std::move(rows));
}

// Instatiate templates.
template NApi::IUnversionedRowsetPtr DeserializeRowset(
    const NProto::TRowsetDescriptor& descriptor,
    const TSharedRef& data);
template NApi::IVersionedRowsetPtr DeserializeRowset(
    const NProto::TRowsetDescriptor& descriptor,
    const TSharedRef& data);

TSharedRef SerializeRowsetWithNameTableDelta(
    const TNameTablePtr& nameTable,
    TRange<TUnversionedRow> rows,
    int* nameTableSize)
{
    NProto::TRowsetDescriptor descriptor;
    const auto& rowRefs = NApi::NRpcProxy::SerializeRowsetWithPartialNameTable(
        nameTable,
        *nameTableSize,
        rows,
        &descriptor);
    *nameTableSize += descriptor.columns_size();

    auto descriptorRef = SerializeProtoToRef(descriptor);
    auto mergedRowRefs = MergeRefsToRef<TRpcProxyRowsetBufferTag>(rowRefs);

    // TODO(kiselyovp) refs are being copied here, we could optimize this
    return PackRefs(std::vector{descriptorRef, mergedRowRefs});
}

TSharedRange<NTableClient::TUnversionedRow> DeserializeRowsetWithNameTableDelta(
    const TSharedRef& data,
    const TNameTablePtr& nameTable,
    NProto::TRowsetDescriptor* descriptor,
    TNameTableToSchemaIdMapping* idMapping)
{
    std::vector<TSharedRef> parts;
    UnpackRefsOrThrow(data, &parts);
    if (parts.size() != 2) {
        THROW_ERROR_EXCEPTION(
            "Error deserializing rowset with name table delta: expected %v packed refs, got %v",
            2,
            parts.size());
    }

    const auto& descriptorDeltaRef = parts[0];
    const auto& mergedRowRefs = parts[1];

    NApi::NRpcProxy::NProto::TRowsetDescriptor descriptorDelta;
    if (!TryDeserializeProto(&descriptorDelta, descriptorDeltaRef)) {
        THROW_ERROR_EXCEPTION("Error deserializing rowset descriptor delta");
    }
    NApi::NRpcProxy::ValidateRowsetDescriptor(
        descriptorDelta,
        NApi::NRpcProxy::CurrentWireFormatVersion,
        NApi::NRpcProxy::NProto::RK_UNVERSIONED);

    auto oldRemoteNameTableSize = descriptor->columns_size();
    descriptor->MergeFrom(descriptorDelta);
    auto newRemoteNameTableSize = descriptor->columns_size();
    auto rowset = NApi::NRpcProxy::DeserializeRowset<TUnversionedRow>(
        *descriptor, mergedRowRefs);

    if (idMapping) {
        idMapping->resize(newRemoteNameTableSize);
        for (int id = oldRemoteNameTableSize; id < newRemoteNameTableSize; ++id) {
            const auto& name = descriptor->columns(id).name();
            (*idMapping)[id] = nameTable->GetIdOrRegisterName(name);
        }

        for (auto row : rowset->GetRows()) {
            auto mutableRow = TMutableUnversionedRow(row.ToTypeErasedRow());
            for (auto& value : mutableRow) {
                auto newId = ApplyIdMapping(value, rowset->Schema(), idMapping);
                if (newId < 0 || newId >= nameTable->GetSize()) {
                    THROW_ERROR_EXCEPTION("Id mapping returned an invalid value %v for id %v: "
                        "expected a value in [0, %v) range",
                        newId,
                        value.Id,
                        nameTable->GetSize());
                }
                value.Id = newId;
            }
        }
    } else {
        for (int id = oldRemoteNameTableSize; id < newRemoteNameTableSize; ++id) {
            const auto& name = descriptor->columns(id).name();
            auto newId = nameTable->RegisterNameOrThrow(name);
            if (newId != id) {
                THROW_ERROR_EXCEPTION("Name table id for name %Qv mismatch: expected %v, got %v",
                    name,
                    id,
                    newId);
            }
        }
    }

    return MakeSharedRange(rowset->GetRows(), rowset);
}

////////////////////////////////////////////////////////////////////////////////

void SortByRegexes(std::vector<TString>& values, const std::vector<NRe2::TRe2Ptr>& regexes)
{
    auto valueToRank = [&] (const TString& value) -> size_t {
        for (size_t index = 0; index < regexes.size(); ++index) {
            if (NRe2::TRe2::FullMatch(NRe2::StringPiece(value), *regexes[index])) {
                return index;
            }
        }
        return regexes.size();
    };
    std::stable_sort(values.begin(), values.end(), [&] (const auto& lhs, const auto& rhs) {
        return valueToRank(lhs) < valueToRank(rhs);
    });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NRpcProxy
