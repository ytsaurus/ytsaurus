#include "client.h"
#include "transaction.h"

#include <yt/yt/client/job_tracker_client/helpers.h>

#include <yt/yt/core/ytree/fluent.h>

#include <yt/yt/core/misc/optional.h>
#include <yt/yt/core/misc/string.h>

namespace NYT::NApi {

using namespace NYTree;
using namespace NJobTrackerClient;

////////////////////////////////////////////////////////////////////////////////

namespace {

EWorkloadCategory FromUserWorkloadCategory(EUserWorkloadCategory category)
{
    switch (category) {
        case EUserWorkloadCategory::Realtime:
            return EWorkloadCategory::UserRealtime;
        case EUserWorkloadCategory::Interactive:
            return EWorkloadCategory::UserInteractive;
        case EUserWorkloadCategory::Batch:
            return EWorkloadCategory::UserBatch;
        default:
            YT_ABORT();
    }
}

} // namespace

TUserWorkloadDescriptor::operator TWorkloadDescriptor() const
{
    TWorkloadDescriptor result;
    result.Category = FromUserWorkloadCategory(Category);
    result.Band = Band;
    return result;
}

struct TSerializableUserWorkloadDescriptor
    : public TYsonSerializableLite
{
    TUserWorkloadDescriptor Underlying;

    TSerializableUserWorkloadDescriptor()
    {
        RegisterParameter("category", Underlying.Category);
        RegisterParameter("band", Underlying.Band)
            .Optional();
    }
};

void Serialize(const TUserWorkloadDescriptor& workloadDescriptor, NYson::IYsonConsumer* consumer)
{
    TSerializableUserWorkloadDescriptor serializableWorkloadDescriptor;
    serializableWorkloadDescriptor.Underlying = workloadDescriptor;
    Serialize(serializableWorkloadDescriptor, consumer);
}

void Deserialize(TUserWorkloadDescriptor& workloadDescriptor, INodePtr node)
{
    TSerializableUserWorkloadDescriptor serializableWorkloadDescriptor;
    Deserialize(serializableWorkloadDescriptor, node);
    workloadDescriptor = serializableWorkloadDescriptor.Underlying;
}

////////////////////////////////////////////////////////////////////////////////

NRpc::TMutationId TMutatingOptions::GetOrGenerateMutationId() const
{
    if (Retry && !MutationId) {
        THROW_ERROR_EXCEPTION("Cannot execute retry without mutation id");
    }
    return MutationId ? MutationId : NRpc::GenerateMutationId();
}

////////////////////////////////////////////////////////////////////////////////

TJournalWriterPerformanceCounters::TJournalWriterPerformanceCounters(const NProfiling::TProfiler& profiler)
{
#define XX(name) \
    name ## Timer = profiler.Timer("/" + CamelCaseToUnderscoreCase(#name) + "_time");

    XX(GetBasicAttributes)
    XX(BeginUpload)
    XX(GetExtendedAttributes)
    XX(GetUploadParameters)
    XX(EndUpload)
    XX(OpenSession)
    XX(CreateChunk)
    XX(AllocateWriteTargets)
    XX(StartNodeSession)
    XX(ConfirmChunk)
    XX(AttachChunk)
    XX(SealChunk)

#undef XX

    WriteQuorumLag = profiler.Timer("/write_quorum_lag");
    MaxReplicaLag = profiler.Timer("/max_replica_lag");
}

////////////////////////////////////////////////////////////////////////////////

TError TCheckPermissionResult::ToError(
    const TString& user,
    EPermission permission,
    const std::optional<TString>& column) const
{
    switch (Action) {
        case NSecurityClient::ESecurityAction::Allow:
            return TError();

        case NSecurityClient::ESecurityAction::Deny: {
            TError error;
            if (ObjectName && SubjectName) {
                error = TError(
                    NSecurityClient::EErrorCode::AuthorizationError,
                    "Access denied for user %Qv: %Qlv permission is denied for %Qv by ACE at %v",
                    user,
                    permission,
                    *SubjectName,
                    *ObjectName);
            } else {
                error = TError(
                    NSecurityClient::EErrorCode::AuthorizationError,
                    "Access denied for user %Qv: %Qlv permission is not allowed by any matching ACE",
                    user,
                    permission);
            }
            error.MutableAttributes()->Set("user", user);
            error.MutableAttributes()->Set("permission", permission);
            if (ObjectId) {
                error.MutableAttributes()->Set("denied_by", ObjectId);
            }
            if (SubjectId) {
                error.MutableAttributes()->Set("denied_for", SubjectId);
            }
            if (column) {
                error.MutableAttributes()->Set("column", *column);
            }
            return error;
        }

        default:
            YT_ABORT();
    }
}

TError TCheckPermissionByAclResult::ToError(const TString &user, EPermission permission) const
{
    switch (Action) {
        case NSecurityClient::ESecurityAction::Allow:
            return TError();

        case NSecurityClient::ESecurityAction::Deny: {
            TError error;
            if (SubjectName) {
                error = TError(
                    NSecurityClient::EErrorCode::AuthorizationError,
                    "Access denied for user %Qv: %Qlv permission is denied for %Qv by ACL",
                    user,
                    permission,
                    *SubjectName);
            } else {
                error = TError(
                    NSecurityClient::EErrorCode::AuthorizationError,
                    "Access denied for user %Qv: %Qlv permission is not allowed by any matching ACE",
                    user,
                    permission);
            }
            error.MutableAttributes()->Set("user", user);
            error.MutableAttributes()->Set("permission", permission);
            if (SubjectId) {
                error.MutableAttributes()->Set("denied_for", SubjectId);
            }
            return error;
        }

        default:
            YT_ABORT();
    }
}

////////////////////////////////////////////////////////////////////////////////

void Serialize(
    const TOperation& operation,
    NYson::IYsonConsumer* consumer,
    bool needType,
    bool needOperationType,
    bool idWithAttributes)
{
    auto fillItems = [&] (TFluentMap fluent) {
        fluent
            .OptionalItem("id", operation.Id)
            .OptionalItem("state", operation.State)
            .DoIf(operation.Type.operator bool(), [&] (TFluentMap fluent) {
                if (needType) {
                    fluent.Item("type").Value(operation.Type);
                }
                if (needOperationType) {
                    fluent.Item("operation_type").Value(operation.Type);
                }
            })
            .OptionalItem("authenticated_user", operation.AuthenticatedUser)
            .OptionalItem("start_time", operation.StartTime)
            .OptionalItem("finish_time", operation.FinishTime)
            .OptionalItem("brief_progress", operation.BriefProgress)
            .OptionalItem("progress", operation.Progress)
            .OptionalItem("brief_spec", operation.BriefSpec)
            .OptionalItem("full_spec", operation.FullSpec)
            .OptionalItem("spec", operation.Spec)
            .OptionalItem("experiment_assignments", operation.ExperimentAssignments)
            .OptionalItem("experiment_assignment_names", operation.ExperimentAssignmentNames)
            .OptionalItem("unrecognized_spec", operation.UnrecognizedSpec)
            .OptionalItem("runtime_parameters", operation.RuntimeParameters)
            .OptionalItem("suspended", operation.Suspended)
            .OptionalItem("result", operation.Result)
            .OptionalItem("events", operation.Events)
            .OptionalItem("slot_index_per_pool_tree", operation.SlotIndexPerPoolTree)
            .OptionalItem("alerts", operation.Alerts)
            .OptionalItem("task_names", operation.TaskNames)
            .DoIf(operation.OtherAttributes.operator bool(), [&] (TFluentMap fluent) {
                for (const auto& [key, value] : operation.OtherAttributes->ListPairs()) {
                    fluent.Item(key).Value(value);
                }
            });
    };

    if (idWithAttributes) {
        if (!operation.Id) {
            THROW_ERROR_EXCEPTION(
                "Cannot serialize operation in id-with-attributes format "
                "as \"id\" attribute is missing from attribute filter");
        }
        BuildYsonFluently(consumer)
            .BeginAttributes()
                .Do(fillItems)
            .EndAttributes()
            .Value(*operation.Id);
    } else {
        BuildYsonFluently(consumer)
            .BeginMap()
                .Do(fillItems)
            .EndMap();
    }
}

void Deserialize(TOperation& operation, NYTree::IAttributeDictionaryPtr attributes, bool clone)
{
    if (clone) {
        attributes = attributes->Clone();
    }

    auto setField = [&] (auto& field, const TString& name) {
        using T = std::remove_reference_t<decltype(field)>;
        if constexpr (std::is_same_v<T, NYson::TYsonString>) {
            if (auto value = attributes->FindYson(name)) {
               field = std::move(value);
               attributes->Remove(name);
            } else {
                field = {};
            }
        } else {
            using TValue = typename TOptionalTraits<T>::TValue;
            if (auto value = attributes->FindAndRemove<TValue>(name)) {
               field = std::move(value);
            } else {
                field.reset();
            }
        }
    };

    setField(operation.Id, "id");
    setField(operation.Type, "type");
    setField(operation.State, "state");
    setField(operation.StartTime, "start_time");
    setField(operation.FinishTime, "finish_time");
    setField(operation.AuthenticatedUser, "authenticated_user");
    setField(operation.BriefSpec, "brief_spec");
    setField(operation.Spec, "spec");
    setField(operation.ExperimentAssignments, "experiment_assignments");
    setField(operation.ExperimentAssignmentNames, "experiment_assignment_names");
    setField(operation.FullSpec, "full_spec");
    setField(operation.UnrecognizedSpec, "unrecognized_spec");
    setField(operation.BriefProgress, "brief_progress");
    setField(operation.Progress, "progress");
    setField(operation.RuntimeParameters, "runtime_parameters");
    setField(operation.Suspended, "suspended");
    setField(operation.Events, "events");
    setField(operation.Result, "result");
    setField(operation.SlotIndexPerPoolTree, "slot_index_per_pool_tree");
    setField(operation.Alerts, "alerts");
    setField(operation.TaskNames, "task_names");

    operation.OtherAttributes = std::move(attributes);
}

////////////////////////////////////////////////////////////////////////////////

std::optional<EJobState> TJob::GetState() const
{
    if (ArchiveState && ControllerAgentState) {
        if (IsJobInProgress(*ArchiveState)) {
            return *ControllerAgentState;
        } else {
            return *ArchiveState;
        }
    } else if (ArchiveState) {
        return *ArchiveState;
    } else if (ControllerAgentState) {
        return *ControllerAgentState;
    }
    return std::nullopt;
}

// Tries to find "abort_reason" attribute in the error and parse it as |EAbortReason|.
// Returns |std::nullopt| if the attribute is not found or any of two parsings is unsuccessful.
static std::optional<NScheduler::EAbortReason> TryGetJobAbortReasonFromError(const NYson::TYsonString& errorYson)
{
    if (!errorYson) {
        return std::nullopt;
    }

    TError error;
    try {
        error = ConvertTo<TError>(errorYson);
    } catch (const TErrorException& exception) {
        return std::nullopt;
    }

    if (auto yson = error.Attributes().FindYson("abort_reason")) {
        try {
            return ConvertTo<NScheduler::EAbortReason>(yson);
        } catch (const TErrorException& exception) {
            return std::nullopt;
        }
    }

    return std::nullopt;
}

void Serialize(const TJob& job, NYson::IYsonConsumer* consumer, TStringBuf idKey)
{

    NYTree::BuildYsonFluently(consumer)
        .BeginMap()
            .OptionalItem(idKey, job.Id)
            .OptionalItem("operation_id", job.OperationId)
            .OptionalItem("type", job.Type)
            .OptionalItem("state", job.GetState())
            .OptionalItem("controller_agent_state", job.ControllerAgentState)
            .OptionalItem("archive_state", job.ArchiveState)
            .OptionalItem("address", job.Address)
            .OptionalItem("start_time", job.StartTime)
            .OptionalItem("finish_time", job.FinishTime)
            .OptionalItem("has_spec", job.HasSpec)
            .OptionalItem("job_competition_id", job.JobCompetitionId)
            .OptionalItem("has_competitors", job.HasCompetitors)
            .OptionalItem("progress", job.Progress)
            .OptionalItem("stderr_size", job.StderrSize)
            .OptionalItem("fail_context_size", job.FailContextSize)
            .OptionalItem("error", job.Error)
            .OptionalItem("abort_reason", TryGetJobAbortReasonFromError(job.Error))
            .OptionalItem("brief_statistics", job.BriefStatistics)
            .OptionalItem("input_paths", job.InputPaths)
            .OptionalItem("core_infos", job.CoreInfos)
            .OptionalItem("events", job.Events)
            .OptionalItem("statistics", job.Statistics)
            .OptionalItem("exec_attributes", job.ExecAttributes)
            .OptionalItem("task_name", job.TaskName)
            .OptionalItem("pool_tree", job.PoolTree)
            .OptionalItem("pool", job.Pool)
            .OptionalItem("monitoring_descriptor", job.MonitoringDescriptor)
            .OptionalItem("is_stale", job.IsStale)
        .EndMap();
}

////////////////////////////////////////////////////////////////////////////////

TFuture<ITransactionPtr> StartAlienTransaction(
    const ITransactionPtr& localTransaction,
    const IClientPtr& alienClient,
    const TAlienTransactionStartOptions& options)
{
    YT_VERIFY(localTransaction->GetType() == NTransactionClient::ETransactionType::Tablet);

    if (localTransaction->GetConnection()->GetClusterId() ==
        alienClient->GetConnection()->GetClusterId())
    {
        return MakeFuture(localTransaction);
    }

    return alienClient->StartTransaction(
        NTransactionClient::ETransactionType::Tablet,
        TTransactionStartOptions{
            .Id = localTransaction->GetId(),
            .Atomicity = options.Atomicity,
            .Durability = options.Durability,
            .StartTimestamp = options.StartTimestamp
        }).Apply(BIND([=] (const ITransactionPtr& alienTransaction) {
            localTransaction->RegisterAlienTransaction(alienTransaction);
            return alienTransaction;
        }));
}

////////////////////////////////////////////////////////////////////////////////

bool TCheckClusterLivenessOptions::IsCheckTrivial() const
{
    return !CheckCypressRoot;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi

