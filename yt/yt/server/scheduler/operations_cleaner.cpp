#include "operations_cleaner.h"

#include "private.h"
#include "operation.h"
#include "bootstrap.h"
#include "scheduler.h"
#include "master_connector.h"
#include "helpers.h"
#include "operation_alert_event.h"

#include <yt/yt/server/lib/scheduler/config.h>
#include <yt/yt/server/lib/scheduler/experiments.h>

#include <yt/yt/ytlib/object_client/object_service_proxy.h>

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/ytlib/controller_agent/helpers.h>

#include <yt/yt/ytlib/scheduler/helpers.h>

#include <yt/yt/ytlib/scheduler/records/operation_alias.record.h>
#include <yt/yt/ytlib/scheduler/records/ordered_by_id.record.h>
#include <yt/yt/ytlib/scheduler/records/ordered_by_start_time.record.h>

#include <yt/yt/client/api/rowset.h>
#include <yt/yt/client/api/transaction.h>

#include <yt/yt/client/security_client/public.h>

#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/row_buffer.h>
#include <yt/yt/client/table_client/helpers.h>
#include <yt/yt/client/table_client/record_helpers.h>

#include <yt/yt/core/actions/cancelable_context.h>

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/nonblocking_batcher.h>
#include <yt/yt/core/concurrency/periodic_executor.h>
#include <yt/yt/core/concurrency/async_semaphore.h>

#include <yt/yt/core/rpc/dispatcher.h>

#include <yt/yt/core/utilex/random.h>

#include <yt/yt/core/yson/string_filter.h>

#include <yt/yt/core/ytree/fluent.h>
#include <yt/yt/core/ytree/ypath_resolver.h>

#include <library/cpp/yt/misc/numeric_helpers.h>

namespace NYT::NScheduler {

using namespace NConcurrency;
using namespace NCypressClient;
using namespace NYTree;
using namespace NYson;
using namespace NObjectClient;
using namespace NApi;
using namespace NTableClient;
using namespace NTransactionClient;
using namespace NProfiling;
using namespace NTracing;

////////////////////////////////////////////////////////////////////////////////

static YT_DEFINE_GLOBAL(const NLogging::TLogger, Logger, "OperationsCleaner");

// TODO(eshcherbin): It should be nested within SchedulerProfiler().
static YT_DEFINE_GLOBAL(const TProfiler, Profiler, TProfiler("/operations_cleaner").WithGlobal());

////////////////////////////////////////////////////////////////////////////////

struct TOrderedByIdTag
{ };

struct TOrderedByStartTimeTag
{ };

struct TOperationAliasesTag
{ };

////////////////////////////////////////////////////////////////////////////////

const std::vector<TString>& TArchiveOperationRequest::GetAttributeKeys()
{
    // Keep the stuff below synchronized with InitializeRequestFromAttributes method.
    static const std::vector<TString> attributeKeys = {
        "key",
        "start_time",
        "finish_time",
        "state",
        "authenticated_user",
        "operation_type",
        "progress",
        "brief_progress",
        "spec",
        "brief_spec",
        "result",
        "events",
        "alerts",
        "full_spec",
        "unrecognized_spec",
        "runtime_parameters",
        "heavy_runtime_parameters",
        "alias",
        "scheduling_attributes_per_pool_tree",
        "slot_index_per_pool_tree",
        "task_names",
        "experiment_assignments",
        "controller_features",
        "provided_spec",
        "temporary_token_node_id",
    };

    return attributeKeys;
}

const std::vector<TString>& TArchiveOperationRequest::GetProgressAttributeKeys()
{
    static const std::vector<TString> attributeKeys = {
        "progress",
        "brief_progress",
    };

    return attributeKeys;
}

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

using TAlertEventsMap = THashMap<EOperationAlertType, std::deque<TOperationAlertEvent>>;

THashMap<TString, TString> GetPoolTreeToPool(const INodePtr& schedulingOptionsNode)
{
    if (!schedulingOptionsNode) {
        return {};
    }

    THashMap<TString, TString> poolTreeToPool;
    for (const auto& [key, value] : schedulingOptionsNode->AsMap()->GetChildren()) {
        poolTreeToPool.emplace(key, value->AsMap()->GetChildValueOrThrow<TString>("pool"));
    }
    return poolTreeToPool;
}

std::vector<TString> GetPools(const INodePtr& schedulingOptionsNode)
{
    std::vector<TString> pools;
    for (const auto& [_, pool] : GetPoolTreeToPool(schedulingOptionsNode)) {
        pools.push_back(pool);
    }
    return pools;
}

template <class T>
static T FilterYsonAndLoadStruct(const TYsonString& source)
{
    auto getPaths = [] {
        std::vector<TString> result;
        for (const auto& [name, _] : T::Fields) {
            result.push_back("/" + name);
        }
        return result;
    };

    static const std::vector<TString> paths = getPaths();

    auto node = ConvertToNode(FilterYsonString(
        paths,
        source,
        /*allowNullResult*/ false))->AsMap();

    T result;
    for (const auto& [name, pointerToField] : T::Fields) {
        result.*pointerToField = node->FindChild(name);
    }
    return result;
}

struct TAnnotationsAndSchedulingOptions
{
    INodePtr Annotations;
    INodePtr SchedulingOptionsPerPoolTree;

    static const inline std::vector<std::pair<TString, INodePtr TAnnotationsAndSchedulingOptions::*>> Fields = {
        {"annotations", &TAnnotationsAndSchedulingOptions::Annotations},
        {"scheduling_options_per_pool_tree", &TAnnotationsAndSchedulingOptions::SchedulingOptionsPerPoolTree},
    };
};

struct TAclAndSchedulingOptions
{
    INodePtr Acl;
    INodePtr AcoName;
    INodePtr SchedulingOptionsPerPoolTree;

    static const inline std::vector<std::pair<TString, INodePtr TAclAndSchedulingOptions::*>> Fields = {
        {"acl", &TAclAndSchedulingOptions::Acl},
        {"aco_name", &TAclAndSchedulingOptions::AcoName},
        {"scheduling_options_per_pool_tree", &TAclAndSchedulingOptions::SchedulingOptionsPerPoolTree},
    };
};

struct TFilteredSpecAttributes
{
    INodePtr Pool;
    INodePtr Title;
    INodePtr InputTablePaths;
    INodePtr OutputTablePaths;
    INodePtr OutputTablePath;
    INodePtr TablePath;

    static const inline std::vector<std::pair<TString, INodePtr TFilteredSpecAttributes::*>> Fields = {
        {"pool", &TFilteredSpecAttributes::Pool},
        {"title", &TFilteredSpecAttributes::Title},
        {"input_table_paths", &TFilteredSpecAttributes::InputTablePaths},
        {"output_table_paths", &TFilteredSpecAttributes::OutputTablePaths},
        {"output_table_path", &TFilteredSpecAttributes::OutputTablePath},
        {"table_path", &TFilteredSpecAttributes::TablePath}
    };
};

std::string GetFilterFactors(const TArchiveOperationRequest& request)
{
    auto getOriginalPath = [] (const INodePtr& node) -> std::optional<TString> {
        try {
            if (auto originalPath = node->Attributes().Find<TString>("original_path")) {
                return *originalPath;
            }
            return NYPath::TRichYPath::Parse(node->AsString()->GetValue()).GetPath();
        } catch (const std::exception& ex) {
            return {};
        }
    };

    auto filteredRuntimeParameters = FilterYsonAndLoadStruct<TAnnotationsAndSchedulingOptions>(request.RuntimeParameters);
    auto filteredSpec = FilterYsonAndLoadStruct<TFilteredSpecAttributes>(request.Spec);

    std::vector<TString> parts;
    parts.push_back(ToString(request.Id));
    parts.push_back(ToString(request.AuthenticatedUser));
    parts.push_back(FormatEnum(request.State));
    parts.push_back(FormatEnum(request.OperationType));

    if (request.ExperimentAssignmentNames) {
        auto experimentAssignmentNames = ConvertTo<std::vector<TString>>(request.ExperimentAssignmentNames);
        parts.insert(parts.end(), experimentAssignmentNames.begin(), experimentAssignmentNames.end());
    }

    if (filteredRuntimeParameters.Annotations) {
        parts.push_back(ConvertToYsonString(filteredRuntimeParameters.Annotations, EYsonFormat::Text).ToString());
    }

    for (const auto& node : {filteredSpec.Pool, filteredSpec.Title}) {
        if (node && node->GetType() == ENodeType::String) {
            parts.push_back(node->AsString()->GetValue());
        }
    }

    for (const auto& node : {filteredSpec.InputTablePaths, filteredSpec.OutputTablePaths}) {
        if (node && node->GetType() == ENodeType::List) {
            auto child = node->AsList()->FindChild(0);
            if (child && child->GetType() == ENodeType::String) {
                if (auto path = getOriginalPath(child)) {
                    parts.push_back(*path);
                }
            }
        }
    }

    for (const auto& node : {filteredSpec.OutputTablePath, filteredSpec.TablePath}) {
        if (node && node->GetType() == ENodeType::String) {
            if (auto path = getOriginalPath(node)) {
                parts.push_back(*path);
            }
        }
    }

    auto pools = GetPools(filteredRuntimeParameters.SchedulingOptionsPerPoolTree);
    parts.insert(parts.end(), pools.begin(), pools.end());

    auto result = JoinToString(parts.begin(), parts.end(), TStringBuf(" "));
    return to_lower(result);
}

bool HasFailedJobs(const TYsonString& briefProgress)
{
    YT_VERIFY(briefProgress);
    auto failedJobs = NYTree::TryGetInt64(briefProgress.AsStringBuf(), "/jobs/failed");
    return failedJobs && *failedJobs > 0;
}

// If progress has state field, we overwrite Archive with Cypress's progress only if operation is finished.
// Otherwise, let's think that information in Archive is the newest (in most cases it is true).
bool NeedProgressInRequest(const TYsonString& progress)
{
    YT_VERIFY(progress);
    auto stateString = NYTree::TryGetString(progress.AsStringBuf(), "/state");
    if (!stateString) {
        return false;
    }
    auto stateEnum = ParseEnum<NControllerAgent::EControllerState>(*stateString);
    return NControllerAgent::IsFinishedState(stateEnum);
}

TUnversionedOwningRow BuildOrderedByIdTableRow(
    const TArchiveOperationRequest& request,
    int version)
{
    auto requestIdAsGuid = request.Id.Underlying();

    NRecords::TOrderedByIdPartial record{
        .Key{
            .IdHi = requestIdAsGuid.Parts64[0],
            .IdLo = requestIdAsGuid.Parts64[1],
        },
        .State = FormatEnum(request.State),
        .AuthenticatedUser = request.AuthenticatedUser,
        .OperationType = FormatEnum(request.OperationType),
        .StartTime = request.StartTime.MicroSeconds(),
        .FinishTime = request.FinishTime.MicroSeconds(),
        .FilterFactors = GetFilterFactors(request),
    };

    if (request.ProvidedSpec) {
        record.ProvidedSpec = request.ProvidedSpec;
    }

    if (request.Spec) {
        record.Spec = request.Spec;
    }

    if (request.FullSpec) {
        record.FullSpec = request.FullSpec;
    }

    if (request.ExperimentAssignments) {
        record.ExperimentAssignments = request.ExperimentAssignments;
    }

    if (request.ExperimentAssignmentNames) {
        record.ExperimentAssignmentNames = request.ExperimentAssignmentNames;
    }

    if (request.BriefSpec) {
        record.BriefSpec = request.BriefSpec;
    }

    if (request.Result) {
        record.Result = request.Result;
    }

    if (request.Events) {
        record.Events = request.Events;
    }

    if (request.Alerts) {
        record.Alerts = request.Alerts;
    }

    if (request.UnrecognizedSpec) {
        record.UnrecognizedSpec = request.UnrecognizedSpec;
    }

    if (request.SlotIndexPerPoolTree) {
        record.SlotIndexPerPoolTree = request.SlotIndexPerPoolTree;
    }

    if (request.TaskNames) {
        record.TaskNames = request.TaskNames;
    }

    if (request.RuntimeParameters) {
        record.RuntimeParameters = request.RuntimeParameters;
    }

    if (request.ControllerFeatures) {
        record.ControllerFeatures = request.ControllerFeatures;
    }

    if (request.Progress && NeedProgressInRequest(request.Progress)) {
        record.Progress = request.Progress;
    }

    if (request.BriefProgress && NeedProgressInRequest(request.BriefProgress)) {
        record.BriefProgress = request.BriefProgress;
    }

    if (version >= 52 && request.SchedulingAttributesPerPoolTree) {
        record.SchedulingAttributesPerPoolTree = request.SchedulingAttributesPerPoolTree;
    }

    return FromRecord(record);
}

TUnversionedOwningRow BuildOrderedByStartTimeTableRow(
    const TArchiveOperationRequest& request,
    int /*version*/)
{
    auto requestIdAsGuid = request.Id.Underlying();
    NRecords::TOrderedByStartTimePartial record{
        .Key{
            .StartTime = static_cast<i64>(request.StartTime.MicroSeconds()),
            .IdHi = requestIdAsGuid.Parts64[0],
            .IdLo = requestIdAsGuid.Parts64[1],
        },
        .OperationType = FormatEnum(request.OperationType),
        .State = FormatEnum(request.State),
        .AuthenticatedUser = request.AuthenticatedUser,
        .FilterFactors = GetFilterFactors(request),
    };

    TYsonString pools;
    TYsonString poolTreeToPool;
    TYsonString acl;

    if (request.RuntimeParameters) {
        auto filteredRuntimeParameters = FilterYsonAndLoadStruct<TAclAndSchedulingOptions>(request.RuntimeParameters);
        pools = ConvertToYsonString(GetPools(filteredRuntimeParameters.SchedulingOptionsPerPoolTree));
        poolTreeToPool = ConvertToYsonString(GetPoolTreeToPool(filteredRuntimeParameters.SchedulingOptionsPerPoolTree));
        if (filteredRuntimeParameters.Acl) {
            acl = ConvertToYsonString(filteredRuntimeParameters.Acl);
        }
    }

    if (pools) {
        record.Pools = pools;
    }

    if (request.BriefProgress) {
        record.HasFailedJobs = HasFailedJobs(request.BriefProgress);
    }

    if (acl) {
        record.Acl = acl;
    }

    if (poolTreeToPool) {
        record.PoolTreeToPool = poolTreeToPool;
    }

    return FromRecord(record);
}

TUnversionedRow BuildOperationAliasesTableRow(
    const TRowBufferPtr& rowBuffer,
    const TArchiveOperationRequest& request,
    int /*version*/)
{
    auto requestIdAsGuid = request.Id.Underlying();
    NRecords::TOperationAlias record{
        .Key = {
            .Alias =  *request.Alias,
        },
        .OperationIdHi = requestIdAsGuid.Parts64[0],
        .OperationIdLo = requestIdAsGuid.Parts64[1],
    };
    return FromRecord(record, rowBuffer);
}

void AddEventToAlertEventsMap(TAlertEventsMap* map, const TOperationAlertEvent& event, int maxAlertEventCountPerAlertType)
{
    auto& events = (*map)[event.AlertType];
    if (event.Error.IsOK() && (events.empty() || events.back().Error.IsOK())) {
        // If previous event is absent/OK, we lost information about current alert
        // due to the archivation queue overflow or scheduler crash.
        // We can do nothing here.
        return;
    }
    if (!event.Error.IsOK() && !events.empty() && !events.back().Error.IsOK()) {
        // Can happen if error message/user-defined error attributes changed or in case of scheduler crashes.
        events.back().Error = event.Error;
    } else {
        events.push_back(event);
    }
    while (std::ssize(events) > maxAlertEventCountPerAlertType) {
        events.pop_front();
    }
}

TAlertEventsMap ConvertToAlertEventsMap(const std::vector<TOperationAlertEvent>& events)
{
    TAlertEventsMap result;
    for (const auto& event : events) {
        result[event.AlertType].push_back(event);
    }
    return result;
}

std::vector<TOperationAlertEvent> ConvertToAlertEvents(const TAlertEventsMap& map)
{
    std::vector<TOperationAlertEvent> result;
    for (const auto& [_, events] : map) {
        result.insert(result.end(), events.begin(), events.end());
    }
    return result;
}

void DoSendOperationAlerts(
    NNative::IClientPtr client,
    std::deque<TOperationAlertEvent> eventsToSend,
    int maxAlertEventCountPerAlertType,
    TDuration transactionTimeout)
{
    YT_LOG_DEBUG("Writing operation alert events to archive (EventCount: %v)", eventsToSend.size());

    auto idMapping = NRecords::TOrderedByIdDescriptor::Get()->GetIdMapping();
    auto columns = std::vector{*idMapping.IdHi, *idMapping.IdLo, *idMapping.AlertEvents};
    auto columnFilter = NTableClient::TColumnFilter(columns);

    THashSet<TOperationId> ids;
    for (const auto& event : eventsToSend) {
        ids.insert(*event.OperationId);
    }
    auto rowsetOrError = LookupOperationsInArchive(
        client,
        std::vector(ids.begin(), ids.end()),
        columnFilter);
    THROW_ERROR_EXCEPTION_IF_FAILED(rowsetOrError, "Failed to fetch operation alert events from archive");
    auto rowset = rowsetOrError.Value();
    auto records = ToOptionalRecords<NRecords::TOrderedByIdPartial>(rowset);

    THashMap<TOperationId, TAlertEventsMap> idToAlertEvents;
    for (auto record : records) {
        if (!record) {
            continue;
        }

        auto operationId = TOperationId(TGuid(record->Key.IdHi, record->Key.IdLo));

        if (record->AlertEvents) {
            idToAlertEvents.emplace(
                operationId,
                ConvertToAlertEventsMap(ConvertTo<std::vector<TOperationAlertEvent>>(*record->AlertEvents)));
        }
    }
    for (const auto& alertEvent : eventsToSend) {
        // Id can be absent in idToAlertEvents if row with such id is not created in archive yet.
        // In this case we want to create this row and initialize it with empty operation alert history.
        YT_VERIFY(alertEvent.OperationId);
        AddEventToAlertEventsMap(&idToAlertEvents[*alertEvent.OperationId], alertEvent, maxAlertEventCountPerAlertType);
    }

    std::vector<TUnversionedRow> rows;
    std::vector<TUnversionedOwningRow> owningRows;
    rows.reserve(idToAlertEvents.size());

    for (const auto& [operationId, eventsMap] : idToAlertEvents) {
        auto operationIdAsGuid = operationId.Underlying();
        NRecords::TOrderedByIdPartial record{
            .Key{
                .IdHi = operationIdAsGuid.Parts64[0],
                .IdLo = operationIdAsGuid.Parts64[1],
            },
            .AlertEvents = ConvertToYsonString(ConvertToAlertEvents(eventsMap)),
        };
        auto row = FromRecord(record);

        rows.push_back(row.Get());
        owningRows.push_back(row);
    }

    TTransactionStartOptions options;
    options.Timeout = transactionTimeout;

    auto transaction = WaitFor(client->StartTransaction(ETransactionType::Tablet, options))
        .ValueOrThrow();
    transaction->WriteRows(
        GetOperationsArchiveOrderedByIdPath(),
        NRecords::TOrderedByIdDescriptor::Get()->GetNameTable(),
        MakeSharedRange(std::move(rows), std::move(owningRows)));

    WaitFor(transaction->Commit())
        .ThrowOnError();

    YT_LOG_DEBUG("Operation alert events written to archive (EventCount: %v)", eventsToSend.size());
}

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

class TOperationsCleaner::TImpl
    : public TRefCounted
{
public:
    DEFINE_SIGNAL(void(const std::vector<TArchiveOperationRequest>&), OperationsRemovedFromCypress);

    TImpl(
        TOperationsCleanerConfigPtr config,
        IOperationsCleanerHost* host,
        TBootstrap* bootstrap)
        : Config_(std::move(config))
        , Bootstrap_(bootstrap)
        , Host_(host)
        , RemoveBatcher_(New<TNonblockingBatcher<TRemoveOperationRequest>>(
            TBatchSizeLimiter(Config_->RemoveBatchSize),
            Config_->RemoveBatchTimeout))
        , ArchiveBatcher_(New<TNonblockingBatcher<TOperationId>>(
            TBatchSizeLimiter(Config_->ArchiveBatchSize),
            Config_->ArchiveBatchTimeout))
        , Client_(Bootstrap_->GetClient()->GetNativeConnection()
            ->CreateNativeClient(TClientOptions::FromUser(NSecurityClient::OperationsCleanerUserName)))
    {
        Profiler().WithTag("locked", "true").AddFuncGauge("/remove_pending", MakeStrong(this), [this] {
            return RemovePendingLocked_.load();
        });
        Profiler().WithTag("locked", "false").AddFuncGauge("/remove_pending", MakeStrong(this), [this] {
            return RemovePending_.load() - RemovePendingLocked_.load();
        });
        Profiler().AddFuncGauge("/archive_pending", MakeStrong(this), [this] {
            return ArchivePending_.load();
        });
        Profiler().AddFuncGauge("/submitted", MakeStrong(this), [this] {
            return Submitted_.load();
        });
        Profiler().AddFuncGauge("/alert_events/enqueued", MakeStrong(this), [this] {
            return EnqueuedAlertEvents_.load();
        });
    }

    void Start()
    {
        YT_ASSERT_THREAD_AFFINITY(ControlThread);

        GetUncancelableInvoker()->Invoke(BIND(&TImpl::DoStart, MakeStrong(this), /*fetchFinishedOperations*/ false));
    }

    void Stop()
    {
        YT_ASSERT_THREAD_AFFINITY(ControlThread);

        GetUncancelableInvoker()->Invoke(BIND(&TImpl::DoStop, MakeStrong(this)));
    }

    void UpdateConfig(const TOperationsCleanerConfigPtr& config)
    {
        YT_ASSERT_THREAD_AFFINITY(ControlThread);

        GetUncancelableInvoker()->Invoke(BIND(&TImpl::DoUpdateConfig, MakeStrong(this), config));
    }

    void SubmitForArchivation(TArchiveOperationRequest request)
    {
        if (!IsEnabled()) {
            return;
        }

        GetCancelableInvoker()->Invoke(
            BIND(&TImpl::DoSubmitForArchivation, MakeStrong(this), Passed(std::move(request))));
    }

    void SubmitForArchivation(std::vector<TOperationId> operationIds)
    {
        if (!IsEnabled()) {
            return;
        }
        YT_LOG_INFO("Operations submitted for fetching by ids and further archivation (OperationCount: %v)",
            operationIds.size());

        BIND(&TImpl::DoFetchFinishedOperationsById, MakeStrong(this))
            .AsyncVia(GetCancelableInvoker())
            .Run(std::move(operationIds))
            .Subscribe(BIND([this, this_ = MakeStrong(this)] (const TError& error) {
                if (!error.IsOK()) {
                    auto disconnectOnFailure = Config_->DisconnectOnFinishedOperationFetchFailure;
                    YT_LOG_WARNING(error, "Failed to fetch finished operations from Cypress (DisconnectOnFailure: %v)",
                        disconnectOnFailure);
                    if (disconnectOnFailure) {
                        Bootstrap_->GetControlInvoker(EControlQueue::MasterConnector)->Invoke(
                            BIND(
                                &TMasterConnector::Disconnect,
                                Bootstrap_->GetScheduler()->GetMasterConnector(),
                                std::move(error)));
                    }
                }
            }).Via(GetCancelableInvoker()));
    }

    void SubmitForRemoval(std::vector<TRemoveOperationRequest> requests)
    {
        if (!IsEnabled()) {
            return;
        }

        GetCancelableInvoker()->Invoke(BIND([this, this_ = MakeStrong(this), requests = std::move(requests)] () mutable {
            for (auto&& request : requests) {
                EnqueueForRemoval(std::move(request));
            }
        }));
    }

    void SetArchiveVersion(int version)
    {
        YT_ASSERT_THREAD_AFFINITY(ControlThread);

        ArchiveVersion_ = version;
    }

    bool IsEnabled() const
    {
        return Enabled_;
    }

    void BuildOrchid(TFluentMap fluent) const
    {
        YT_ASSERT_THREAD_AFFINITY(ControlThread);

        fluent
            .Item("enable").Value(IsEnabled())
            .Item("enable_operation_archivation").Value(IsOperationArchivationEnabled())
            .Item("remove_pending").Value(RemovePending_.load())
            .Item("archive_pending").Value(ArchivePending_.load())
            .Item("submitted").Value(Submitted_.load())
            .Item("enqueued_alert_events").Value(EnqueuedAlertEvents_.load())
            .Item("remove_pending_locked").Value(RemovePendingLocked_.load());
    }

    void EnqueueOperationAlertEvent(
        TOperationId operationId,
        EOperationAlertType alertType,
        TError alert)
    {
        YT_ASSERT_THREAD_AFFINITY(ControlThread);

        if (!IsEnabled()) {
            return;
        }

        GetCancelableInvoker()->Invoke(
            BIND([this, this_ = MakeStrong(this), operationId, alertType, alert = std::move(alert)] {
                OperationAlertEventQueue_.push_back({
                    operationId,
                    alertType,
                    alert.HasDatetime() ? alert.GetDatetime() : TInstant::Now(),
                    alert,
                });
                CheckAndTruncateAlertEvents();
            }));
    }

    TArchiveOperationRequest InitializeRequestFromOperation(const TOperationPtr& operation)
    {
        TArchiveOperationRequest result;

        if (operation->GetRuntimeParameters() && operation->GetRuntimeParameters()->AcoName) {
            auto acl = GetAclFromAcoName(Bootstrap_->GetClient(), *operation->GetRuntimeParameters()->AcoName);
            operation->GetRuntimeParameters()->Acl = ConvertTo<NSecurityClient::TSerializableAccessControlList>(acl);
        }

        result.Id = operation->GetId();
        result.StartTime = operation->GetStartTime();
        result.FinishTime = *operation->GetFinishTime();
        result.State = operation->GetState();
        result.AuthenticatedUser = operation->GetAuthenticatedUser();
        result.OperationType = operation->GetType();
        result.Spec = operation->GetSpecString();
        result.Result = operation->BuildResultString();
        result.Events = ConvertToYsonString(operation->Events());
        result.Alerts = operation->BuildAlertsString();
        result.BriefSpec = operation->BriefSpecString();
        result.RuntimeParameters = ConvertToYsonString(operation->GetRuntimeParameters(), EYsonFormat::Binary);
        result.Alias = operation->Alias();
        result.SchedulingAttributesPerPoolTree = ConvertToYsonString(operation->GetSchedulingAttributesPerPoolTree(), EYsonFormat::Binary);
        result.SlotIndexPerPoolTree = ConvertToYsonString(operation->GetSlotIndices(), EYsonFormat::Binary);
        result.TaskNames = ConvertToYsonString(operation->GetTaskNames(), EYsonFormat::Binary);
        result.ExperimentAssignments = ConvertToYsonString(operation->ExperimentAssignments(), EYsonFormat::Binary);
        result.ExperimentAssignmentNames = ConvertToYsonString(operation->GetExperimentAssignmentNames(), EYsonFormat::Binary);
        result.ProvidedSpec = operation->ProvidedSpecString();

        const auto& attributes = operation->ControllerAttributes();
        const auto& initializationAttributes = attributes.InitializeAttributes;
        if (initializationAttributes) {
            result.UnrecognizedSpec = initializationAttributes->UnrecognizedSpec;
            result.FullSpec = initializationAttributes->FullSpec;
        }

        result.DependentNodeIds = operation->GetDependentNodeIds();

        return result;
    }

    TArchiveOperationRequest InitializeRequestFromAttributes(const IAttributeDictionary& attributes)
    {
        TArchiveOperationRequest result;

        result.Id = TOperationId(TGuid::FromString(attributes.Get<TString>("key")));
        result.StartTime = attributes.Get<TInstant>("start_time");
        result.FinishTime = attributes.Get<TInstant>("finish_time");
        result.State = attributes.Get<EOperationState>("state");
        result.AuthenticatedUser = attributes.Get<TString>("authenticated_user");
        result.OperationType = attributes.Get<EOperationType>("operation_type");
        result.Progress = attributes.FindYson("progress");
        result.BriefProgress = attributes.FindYson("brief_progress");
        result.Spec = attributes.GetYson("spec");
        // In order to recover experiment assignment names, we must either
        // dig into assignment YSON representation or reconstruct assignment objects.
        // The latter seems more convenient. Also, do not forget that older operations
        // may miss assignment attribute at all.
        if (auto experimentAssignmentsYson = attributes.FindYson("experiment_assignments")) {
            result.ExperimentAssignments = experimentAssignmentsYson;
            auto experimentAssignments = ConvertTo<std::vector<TExperimentAssignmentPtr>>(experimentAssignmentsYson);
            std::vector<TString> experimentAssignmentNames;
            experimentAssignmentNames.reserve(experimentAssignments.size());
            for (const auto& experimentAssignment : experimentAssignments) {
                experimentAssignmentNames.emplace_back(experimentAssignment->GetName());
            }
            result.ExperimentAssignmentNames = ConvertToYsonString(experimentAssignmentNames, EYsonFormat::Binary);
        }

        result.ProvidedSpec = attributes.FindYson("provided_spec");
        result.BriefSpec = attributes.FindYson("brief_spec");
        result.Result = attributes.GetYson("result");
        result.Events = attributes.GetYson("events");
        result.Alerts = attributes.GetYson("alerts");
        result.FullSpec = attributes.FindYson("full_spec");
        result.UnrecognizedSpec = attributes.FindYson("unrecognized_spec");

        if (auto heavyRuntimeParameters = attributes.Find<IMapNodePtr>("heavy_runtime_parameters")) {
            auto runtimeParameters = attributes.Find<IMapNodePtr>("runtime_parameters");
            if (!runtimeParameters) {
                result.RuntimeParameters = ConvertToYsonString(heavyRuntimeParameters);
            } else {
                result.RuntimeParameters = ConvertToYsonString(PatchNode(runtimeParameters, heavyRuntimeParameters));
            }
        } else {
            result.RuntimeParameters = attributes.FindYson("runtime_parameters");
        }
        result.Alias = ConvertTo<TOperationSpecBasePtr>(result.Spec)->Alias;
        result.SchedulingAttributesPerPoolTree = attributes.FindYson("scheduling_attributes_per_pool_tree");
        result.SlotIndexPerPoolTree = attributes.FindYson("slot_index_per_pool_tree");
        result.TaskNames = attributes.FindYson("task_names");
        result.ControllerFeatures = attributes.FindYson("controller_features");

        if (auto temporaryTokenNodeId = attributes.Find<TNodeId>("temporary_token_node_id")) {
            result.DependentNodeIds = {*temporaryTokenNodeId};
        }

        return result;
    }


private:
    TOperationsCleanerConfigPtr Config_;
    TBootstrap* const Bootstrap_;
    IOperationsCleanerHost* const Host_;

    TPeriodicExecutorPtr AnalysisExecutor_;
    TPeriodicExecutorPtr OperationAlertEventSenderExecutor_;

    TCancelableContextPtr CancelableContext_;
    IInvokerPtr CancelableInvoker_;

    std::atomic<i64> ArchiveVersion_{-1};

    std::atomic<bool> Enabled_{false};
    std::atomic<bool> OperationArchivationEnabled_{false};

    TDelayedExecutorCookie OperationArchivationStartCookie_;

    std::multimap<TInstant, TOperationId> ArchiveTimeToOperationIdMap_;
    THashMap<TOperationId, TArchiveOperationRequest> OperationMap_;

    // Removals might be issued for operations without an entry in OperationMap_,
    // so we need to store the complete removal request in the queue.
    TIntrusivePtr<TNonblockingBatcher<TRemoveOperationRequest>> RemoveBatcher_;
    // We can store plain operation ids here because operations going
    // through archivation have a corresponding entry in OperationMap_.
    TIntrusivePtr<TNonblockingBatcher<TOperationId>> ArchiveBatcher_;
    std::deque<std::pair<TRemoveOperationRequest, TInstant>> LockedOperationQueue_;

    std::deque<TOperationAlertEvent> OperationAlertEventQueue_;
    TInstant LastOperationAlertEventSendTime_;

    NNative::IClientPtr Client_;

    std::atomic<i64> RemovePending_{0};
    std::atomic<i64> ArchivePending_{0};
    std::atomic<i64> Submitted_{0};
    std::atomic<i64> EnqueuedAlertEvents_{0};
    std::atomic<i64> RemovePendingLocked_{0};

    TCounter ArchivedOperationCounter_ = Profiler().Counter("/archived");
    TCounter RemovedOperationCounter_ = Profiler().Counter("/removed");
    TCounter DroppedOperationCounter_ = Profiler().Counter("/dropped");
    TCounter CommittedDataWeightCounter_ = Profiler().Counter("/committed_data_weight");
    TCounter ArchiveErrorCounter_ = Profiler().Counter("/archive_errors");
    TCounter RemoveOperationErrorCounter_ = Profiler().Counter("/remove_errors");
    TCounter ArchivedOperationAlertEventCounter_ = Profiler().Counter("/alert_events/archived");
    TCounter DroppedOperationAlertEventCounter_ = Profiler().Counter("/alert_events/dropped");
    TEventTimer AnalyzeOperationsTimer_ = Profiler().Timer("/analyze_operations_time");
    TEventTimer OperationsRowsPreparationTimer_ = Profiler().Timer("/operations_rows_preparation_time");

    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);

private:
    IInvokerPtr GetCancelableInvoker() const
    {
        return CancelableInvoker_;
    }

    IInvokerPtr GetUncancelableInvoker() const
    {
        return Host_->GetOperationsCleanerInvoker();
    }

    void ScheduleArchiveOperations()
    {
        GetCancelableInvoker()->Invoke(BIND(&TImpl::ArchiveOperations, MakeStrong(this)));
    }

    void SetSchedulerAlert(ESchedulerAlertType alertType, const TError& alert)
    {
        YT_ASSERT_INVOKER_AFFINITY(GetUncancelableInvoker());

        Bootstrap_->GetControlInvoker(EControlQueue::OperationsCleaner)->Invoke(
            BIND(&IOperationsCleanerHost::SetSchedulerAlert, Host_, alertType, alert));
    }

    void DoStart(bool fetchFinishedOperations)
    {
        YT_ASSERT_INVOKER_AFFINITY(GetUncancelableInvoker());

        if (!Config_->Enable || Enabled_) {
            return;
        }

        Enabled_ = true;

        YT_VERIFY(!CancelableContext_);
        CancelableContext_ = New<TCancelableContext>();
        CancelableInvoker_ = CancelableContext_->CreateInvoker(GetUncancelableInvoker());

        AnalysisExecutor_ = New<TPeriodicExecutor>(
            CancelableInvoker_,
            BIND(&TImpl::OnAnalyzeOperations, MakeWeak(this)),
            Config_->AnalysisPeriod);

        AnalysisExecutor_->Start();

        GetCancelableInvoker()->Invoke(BIND(&TImpl::RemoveOperations, MakeStrong(this)));

        ScheduleArchiveOperations();
        DoStartOperationArchivation();
        DoStartAlertEventArchivation();

        // If operations cleaner was disabled during scheduler runtime and then
        // enabled then we should fetch all finished operation since scheduler did not
        // reported them.
        if (fetchFinishedOperations) {
            GetCancelableInvoker()->Invoke(BIND(&TImpl::FetchFinishedOperations, MakeStrong(this)));
        }

        YT_LOG_INFO("Operations cleaner started");
    }

    void DoStartOperationArchivation()
    {
        if (Config_->Enable && Config_->EnableOperationArchivation && !OperationArchivationEnabled_) {
            OperationArchivationEnabled_ = true;
            TDelayedExecutor::CancelAndClear(OperationArchivationStartCookie_);
            SetSchedulerAlert(ESchedulerAlertType::OperationsArchivation, TError());

            YT_LOG_INFO("Operations archivation started");
        }
    }

    void DoStartAlertEventArchivation()
    {
        if (Config_->Enable && Config_->EnableOperationAlertEventArchivation && !OperationAlertEventSenderExecutor_) {
            OperationAlertEventSenderExecutor_ = New<TPeriodicExecutor>(
                CancelableInvoker_,
                BIND(&TImpl::SendOperationAlerts, MakeWeak(this)),
                Config_->OperationAlertEventSendPeriod);
            OperationAlertEventSenderExecutor_->Start();

            YT_LOG_INFO("Alert event archivation started");
        }
    }

    void DoStopOperationArchivation()
    {
        if (!OperationArchivationEnabled_) {
            return;
        }

        OperationArchivationEnabled_ = false;
        TDelayedExecutor::CancelAndClear(OperationArchivationStartCookie_);
        SetSchedulerAlert(ESchedulerAlertType::OperationsArchivation, TError());

        YT_LOG_INFO("Operations archivation stopped");
    }

    void DoStopAlertEventArchivation()
    {
        if (!OperationAlertEventSenderExecutor_) {
            return;
        }
        YT_UNUSED_FUTURE(OperationAlertEventSenderExecutor_->Stop());
        OperationAlertEventSenderExecutor_.Reset();

        YT_LOG_INFO("Alert event archivation stopped");
    }

    void DoStop()
    {
        YT_ASSERT_INVOKER_AFFINITY(GetUncancelableInvoker());
        if (!Enabled_) {
            return;
        }

        Enabled_ = false;

        if (CancelableContext_) {
            CancelableContext_->Cancel(TError("Operation cleaner stopped"));
        }
        CancelableContext_.Reset();

        CancelableInvoker_ = nullptr;

        if (AnalysisExecutor_) {
            YT_UNUSED_FUTURE(AnalysisExecutor_->Stop());
        }
        AnalysisExecutor_.Reset();

        TDelayedExecutor::CancelAndClear(OperationArchivationStartCookie_);

        DoStopOperationArchivation();
        DoStopAlertEventArchivation();

        ArchiveBatcher_->Drop();
        RemoveBatcher_->Drop();
        ArchiveTimeToOperationIdMap_.clear();
        OperationMap_.clear();
        OperationAlertEventQueue_.clear();
        LockedOperationQueue_.clear();
        ArchivePending_ = 0;
        RemovePending_ = 0;
        RemovePendingLocked_ = 0;

        YT_LOG_INFO("Operations cleaner stopped");
    }

    void DoUpdateConfig(TOperationsCleanerConfigPtr config)
    {
        YT_ASSERT_INVOKER_AFFINITY(GetUncancelableInvoker());

        bool oldEnable = Config_->Enable;
        bool oldEnableOperationArchivation = Config_->EnableOperationArchivation;
        bool oldEnableOperationAlertEventArchivation = Config_->EnableOperationAlertEventArchivation;
        Config_ = std::move(config);

        if (oldEnable != Config_->Enable) {
            if (Config_->Enable) {
                DoStart(/*fetchFinishedOperations*/ true);
            } else {
                DoStop();
            }
        }

        if (oldEnableOperationArchivation != Config_->EnableOperationArchivation) {
            if (Config_->EnableOperationArchivation) {
                DoStartOperationArchivation();
            } else {
                DoStopOperationArchivation();
            }
        }

        if (oldEnableOperationAlertEventArchivation != Config_->EnableOperationAlertEventArchivation) {
            if (Config_->EnableOperationAlertEventArchivation) {
                DoStartAlertEventArchivation();
            } else {
                DoStopAlertEventArchivation();
            }
        }

        CheckAndTruncateAlertEvents();
        if (OperationAlertEventSenderExecutor_) {
            OperationAlertEventSenderExecutor_->SetPeriod(Config_->OperationAlertEventSendPeriod);
        }

        if (AnalysisExecutor_) {
            AnalysisExecutor_->SetPeriod(Config_->AnalysisPeriod);
        }

        ArchiveBatcher_->UpdateBatchLimiter(TBatchSizeLimiter(Config_->ArchiveBatchSize));
        ArchiveBatcher_->UpdateBatchDuration(Config_->ArchiveBatchTimeout);

        RemoveBatcher_->UpdateBatchLimiter(TBatchSizeLimiter(Config_->RemoveBatchSize));
        RemoveBatcher_->UpdateBatchDuration(Config_->RemoveBatchTimeout);

        YT_LOG_INFO("Operations cleaner config updated (Enable: %v, EnableOperationArchivation: %v, EnableOperationAlertEventArchivation: %v)",
            Config_->Enable,
            Config_->EnableOperationArchivation,
            Config_->EnableOperationAlertEventArchivation);
    }

    void DoSubmitForArchivation(TArchiveOperationRequest request)
    {
        YT_ASSERT_INVOKER_AFFINITY(GetCancelableInvoker());

        auto id = request.Id;

        // Can happen if scheduler reported operation and archiver was turned on and
        // fetched the same operation from Cypress.
        if (OperationMap_.find(id) != OperationMap_.end()) {
            return;
        }

        auto deadline = request.FinishTime + Config_->CleanDelay;

        ArchiveTimeToOperationIdMap_.emplace(deadline, id);
        EmplaceOrCrash(OperationMap_, id, std::move(request));

        ++Submitted_;

        YT_LOG_DEBUG("Operation submitted for archivation (OperationId: %v, ArchivationStartTime: %v)",
            id,
            deadline);
    }

    void OnAnalyzeOperations()
    {
        YT_ASSERT_INVOKER_AFFINITY(GetCancelableInvoker());

        YT_LOG_INFO("Analyzing operations submitted for archivation (SubmittedOperationCount: %v)",
            ArchiveTimeToOperationIdMap_.size());

        if (ArchiveTimeToOperationIdMap_.empty()) {
            YT_LOG_INFO("No operations submitted for archivation");
            return;
        }

        auto now = TInstant::Now();

        int retainedCount = 0;
        int enqueuedForArchivationCount = 0;
        THashMap<TString, int> operationCountPerUser;

        auto canArchive = [&] (const auto& request) {
            if (retainedCount >= Config_->HardRetainedOperationCount) {
                return true;
            }

            if (now - request.FinishTime > Config_->MaxOperationAge) {
                return true;
            }

            if (operationCountPerUser[request.AuthenticatedUser] >= Config_->MaxOperationCountPerUser) {
                return true;
            }

            // TODO(asaitgalin): Consider only operations without stderrs?
            if (retainedCount >= Config_->SoftRetainedOperationCount &&
                request.State != EOperationState::Failed)
            {
                return true;
            }

            return false;
        };

        // Analyze operations with expired grace timeout, from newest to oldest.
        {
            TEventTimerGuard guard(AnalyzeOperationsTimer_);

            auto it = ArchiveTimeToOperationIdMap_.lower_bound(now);
            while (it != ArchiveTimeToOperationIdMap_.begin()) {
                --it;

                auto operationId = it->second;
                const auto& request = GetRequest(operationId);
                if (canArchive(request)) {
                    it = ArchiveTimeToOperationIdMap_.erase(it);
                    CleanOperation(operationId);
                    enqueuedForArchivationCount += 1;
                } else {
                    retainedCount += 1;
                    operationCountPerUser[request.AuthenticatedUser] += 1;
                }
            }
        }

        Submitted_.store(ArchiveTimeToOperationIdMap_.size());

        YT_LOG_INFO(
            "Finished analyzing operations submitted for archivation "
            "(RetainedCount: %v, EnqueuedForArchivationCount: %v)",
            retainedCount,
            enqueuedForArchivationCount);
    }

    void EnqueueForRemoval(TRemoveOperationRequest request)
    {
        YT_ASSERT_INVOKER_AFFINITY(GetUncancelableInvoker());

        YT_LOG_DEBUG("Operation enqueued for removal (OperationId: %v, DependentNodeIds: %v)", request.Id, request.DependentNodeIds);
        RemovePending_++;
        RemoveBatcher_->Enqueue(std::move(request));
    }

    void EnqueueForArchivation(TOperationId operationId)
    {
        YT_ASSERT_INVOKER_AFFINITY(GetCancelableInvoker());

        YT_LOG_DEBUG("Operation enqueued for archivation (OperationId: %v)", operationId);
        ArchivePending_++;
        ArchiveBatcher_->Enqueue(operationId);
    }

    void CleanOperation(TOperationId operationId)
    {
        YT_ASSERT_INVOKER_AFFINITY(GetCancelableInvoker());

        if (IsOperationArchivationEnabled()) {
            EnqueueForArchivation(operationId);
        } else {
            // This method is only called for operations that went through the SubmitForArchivation
            // pipeline, so it is safe to assume that it is present in OperationMap_.
            EnqueueForRemoval(GetRequest(operationId));

            DroppedOperationCounter_.Increment();
        }
    }

    void TryArchiveOperations(const std::vector<TOperationId>& operationIds)
    {
        YT_ASSERT_INVOKER_AFFINITY(GetCancelableInvoker());

        int version = ArchiveVersion_;
        if (version == -1) {
            THROW_ERROR_EXCEPTION("Unknown operations archive version");
        }

        TTransactionStartOptions options;
        options.Timeout = Config_->TabletTransactionTimeout;

        auto asyncTransaction = Client_->StartTransaction(ETransactionType::Tablet, options);
        auto transaction = WaitFor(asyncTransaction)
            .ValueOrThrow();

        YT_LOG_DEBUG(
            "Operations archivation transaction started (TransactionId: %v, OperationCount: %v)",
            transaction->GetId(),
            operationIds.size());

        i64 orderedByIdRowsDataWeight = 0;
        i64 orderedByStartTimeRowsDataWeight = 0;
        i64 operationAliasesRowsDataWeight = 0;

        THashSet<TOperationId> skippedOperationIds;

        auto isValueWeightViolated = [&] (TUnversionedRow row, TOperationId operationId, const TNameTablePtr nameTable) {
            for (auto value : row) {
                auto valueWeight = GetDataWeight(value);
                if (valueWeight > MaxStringValueLength) {
                    YT_LOG_WARNING(
                        "Operation row violates value data weight, archivation skipped"
                        "(OperationId: %v, Key: %v, Weight: %v, WeightLimit: %v)",
                        operationId,
                        nameTable->GetNameOrThrow(value.Id),
                        valueWeight,
                        MaxStringValueLength);
                    return true;
                }
            }
            return false;
        };

        {
            TEventTimerGuard guard(OperationsRowsPreparationTimer_);

            // ordered_by_id table rows
            {
                auto nameTable = NRecords::TOrderedByIdDescriptor::Get()->GetNameTable();
                std::vector<TUnversionedRow> rows;
                std::vector<TUnversionedOwningRow> owningRows;
                rows.reserve(operationIds.size());

                for (auto operationId : operationIds) {
                    try {
                        const auto& request = GetRequest(operationId);
                        auto row = NDetail::BuildOrderedByIdTableRow(request, version);

                        if (isValueWeightViolated(row, operationId, nameTable)) {
                            skippedOperationIds.insert(operationId);
                            continue;
                        }

                        rows.push_back(row.Get());
                        owningRows.push_back(row);
                        orderedByIdRowsDataWeight += GetDataWeight(row);
                    } catch (const std::exception& ex) {
                        THROW_ERROR_EXCEPTION("Failed to build row for operation %v", operationId)
                            << ex;
                    }
                }

                transaction->WriteRows(
                    GetOperationsArchiveOrderedByIdPath(),
                    nameTable,
                    MakeSharedRange(std::move(rows), std::move(owningRows)));
            }

            // ordered_by_start_time rows
            {
                auto nameTable = NRecords::TOrderedByStartTimeDescriptor::Get()->GetNameTable();
                std::vector<TUnversionedRow> rows;
                std::vector<TUnversionedOwningRow> owningRows;
                rows.reserve(operationIds.size());
                owningRows.reserve(operationIds.size());

                for (auto operationId : operationIds) {
                    if (skippedOperationIds.contains(operationId)) {
                        continue;
                    }
                    try {
                        const auto& request = GetRequest(operationId);
                        auto row = NDetail::BuildOrderedByStartTimeTableRow(request, version);
                        rows.push_back(row.Get());
                        owningRows.push_back(row);

                        orderedByStartTimeRowsDataWeight += GetDataWeight(row);
                    } catch (const std::exception& ex) {
                        THROW_ERROR_EXCEPTION("Failed to build row for operation %v", operationId)
                            << ex;
                    }
                }

                transaction->WriteRows(
                    GetOperationsArchiveOrderedByStartTimePath(),
                    nameTable,
                    MakeSharedRange(std::move(rows), std::move(owningRows)));
            }

            // operation_aliases rows
            {
                auto rowBuffer = New<TRowBuffer>(TOperationAliasesTag{});
                std::vector<TUnversionedRow> rows;
                rows.reserve(operationIds.size());

                for (auto operationId : operationIds) {
                    if (skippedOperationIds.contains(operationId)) {
                        continue;
                    }

                    const auto& request = GetRequest(operationId);
                    if (request.Alias) {
                        auto row = NDetail::BuildOperationAliasesTableRow(rowBuffer, request, version);
                        rows.push_back(row);
                        operationAliasesRowsDataWeight += GetDataWeight(row);
                    }
                }

                transaction->WriteRows(
                    GetOperationsArchiveOperationAliasesPath(),
                    NRecords::TOperationAliasDescriptor::Get()->GetNameTable(),
                    MakeSharedRange(std::move(rows), std::move(rowBuffer)));
            }
        }

        i64 totalDataWeight = orderedByIdRowsDataWeight + orderedByStartTimeRowsDataWeight;

        YT_LOG_DEBUG(
            "Started committing archivation transaction (TransactionId: %v, OperationCount: %v, SkippedOperationCount: %v, "
            "OrderedByIdRowsDataWeight: %v, OrderedByStartTimeRowsDataWeight: %v, OperationAliasesRowsDataWeight: %v, "
            "TotalDataWeight: %v)",
            transaction->GetId(),
            operationIds.size(),
            skippedOperationIds.size(),
            orderedByIdRowsDataWeight,
            orderedByStartTimeRowsDataWeight,
            operationAliasesRowsDataWeight,
            totalDataWeight);

        WaitFor(transaction->Commit())
            .ThrowOnError();

        YT_LOG_DEBUG("Finished committing archivation transaction (TransactionId: %v)", transaction->GetId());

        YT_LOG_DEBUG("Operations archived (OperationIds: %v)", operationIds);

        CommittedDataWeightCounter_.Increment(totalDataWeight);
        ArchivedOperationCounter_.Increment(operationIds.size());
    }

    bool IsOperationArchivationEnabled() const
    {
        return IsEnabled() && OperationArchivationEnabled_;
    }

    void ArchiveOperations()
    {
        YT_ASSERT_INVOKER_AFFINITY(GetCancelableInvoker());

        auto batch = WaitFor(ArchiveBatcher_->DequeueBatch())
            .ValueOrThrow();

        if (!batch.empty()) {
            while (IsOperationArchivationEnabled()) {
                TError error;
                {
                    TTraceContextGuard traceContextGuard(TTraceContext::NewRoot("ArchiveOperations"));

                    try {
                        TryArchiveOperations(batch);
                    } catch (const std::exception& ex) {
                        int pendingCount = ArchivePending_.load();
                        error = TError("Failed to archive operations")
                            << TErrorAttribute("pending_count", pendingCount)
                            << ex;
                        YT_LOG_WARNING(error);
                        ArchiveErrorCounter_.Increment();
                    }
                }

                int pendingCount = ArchivePending_.load();
                if (pendingCount >= Config_->MinOperationCountEnqueuedForAlert) {
                    auto alertError = TError("Too many operations in archivation queue")
                        << TErrorAttribute("pending_count", pendingCount);
                    if (!error.IsOK()) {
                        alertError.MutableInnerErrors()->push_back(error);
                    }
                    SetSchedulerAlert(
                        ESchedulerAlertType::OperationsArchivation,
                        alertError);
                } else {
                    SetSchedulerAlert(
                        ESchedulerAlertType::OperationsArchivation,
                        TError());
                }

                if (error.IsOK()) {
                    break;
                }

                if (ArchivePending_ > Config_->MaxOperationCountEnqueuedForArchival) {
                    TemporarilyDisableArchivation();
                    break;
                } else {
                    auto sleepDelay = Config_->MinArchivationRetrySleepDelay +
                        RandomDuration(Config_->MaxArchivationRetrySleepDelay - Config_->MinArchivationRetrySleepDelay);
                    TDelayedExecutor::WaitForDuration(sleepDelay);
                }
            }

            for (auto operationId : batch) {
                // All of these operations went through the SubmitForArchivation pipeline,
                // so it is safe to assume that it is present in OperationMap_.
                EnqueueForRemoval(GetRequest(operationId));
            }

            ArchivePending_ -= batch.size();
        }

        ScheduleArchiveOperations();
    }

    void DoRemoveOperations(std::vector<TRemoveOperationRequest> requests)
    {
        YT_LOG_DEBUG("Removing operations from Cypress (OperationCount: %v)", requests.size());

        ProcessWaitingLockedOperations();

        // These requests will be requeued and retried later.
        std::vector<TRemoveOperationRequest> failedRequests;
        // List of requests for which all dependent nodes and the operation node were removed successfully.
        std::vector<TRemoveOperationRequest> successfulRequests;

        // Intermediate list of requests used to remove dependent nodes first.
        // If all dependent nodes are removed successfully, the request is moved to the final removal list below.
        std::vector<TRemoveOperationRequest> requestsWithDependentNodesToRemove;
        // Intermediate list of requests for which there are no operation node locks and all dependent nodes were removed, if present.
        std::vector<TRemoveOperationRequest> requestsWithOperationNodeToRemove;

        int lockedOperationCount = 0;
        int failedToRemoveOperationCount = 0;

        // Fetch lock_count attribute.
        {
            auto proxy = CreateObjectServiceReadProxy(
                Client_,
                EMasterChannelKind::Follower);
            auto batchReq = proxy.ExecuteBatch();

            for (const auto& removeOperationRequest : requests) {
                auto req = TYPathProxy::Get(GetOperationPath(removeOperationRequest.Id) + "/@lock_count");
                batchReq->AddRequest(req, "get_lock_count");
            }

            auto batchRspOrError = WaitFor(batchReq->Invoke());

            if (batchRspOrError.IsOK()) {
                const auto& batchRsp = batchRspOrError.Value();
                auto rsps = batchRsp->GetResponses<TYPathProxy::TRspGet>("get_lock_count");
                YT_VERIFY(std::ssize(rsps) == std::ssize(requests));

                auto now = TInstant::Now();
                for (int index = 0; index < std::ssize(rsps); ++index) {
                    bool isLocked = false;
                    const auto rsp = rsps[index];
                    if (rsp.IsOK()) {
                        auto lockCountNode = ConvertToNode(TYsonString(rsp.Value()->value()));
                        if (lockCountNode->AsUint64()->GetValue() > 0) {
                            isLocked = true;
                        }
                    }

                    auto& removeOperationRequest = requests[index];
                    if (isLocked) {
                        LockedOperationQueue_.emplace_back(std::move(removeOperationRequest), now);
                        ++lockedOperationCount;
                    } else if (!removeOperationRequest.DependentNodeIds.empty()) {
                        requestsWithDependentNodesToRemove.push_back(std::move(removeOperationRequest));
                    } else {
                        requestsWithOperationNodeToRemove.push_back(std::move(removeOperationRequest));
                    }
                }
            } else {
                YT_LOG_WARNING(
                    batchRspOrError,
                    "Failed to get lock count for operations from Cypress (OperationCount: %v)",
                    requests.size());

                failedRequests = requests;

                failedToRemoveOperationCount = std::ssize(requests);
            }
        }

        // Remove dependent nodes from operations that have them.
        // If all dependent nodes are removed successfully, the operation node itself will be removed in the next step.
        // Otherwise, the whole removal process for the operation will be retried later.
        {
            auto proxy = CreateObjectServiceWriteProxy(Client_);
            // TODO(achulkov2): Split into subbatches as it is done below if we ever need it.
            auto batchReq = proxy.ExecuteBatch();

            i64 totalDependentNodeCount = 0;
            for (const auto& operationId : requestsWithDependentNodesToRemove) {
                YT_VERIFY(!operationId.DependentNodeIds.empty());

                for (const auto& dependentNodeId : operationId.DependentNodeIds) {
                    ++totalDependentNodeCount;
                    auto req = TYPathProxy::Remove(FromObjectId(dependentNodeId));
                    req->set_force(true);
                    batchReq->AddRequest(req);
                }
            }

            YT_LOG_DEBUG(
                "Removing dependent nodes from operations (OperationCount: %v, DependentNodeCount: %v)",
                requestsWithDependentNodesToRemove.size(),
                totalDependentNodeCount);

            auto batchRspOrError = WaitFor(batchReq->Invoke());

            i64 allDependentNodesRemovedRequestCount = 0;

            if (batchRspOrError.IsOK()) {
                auto responses = batchRspOrError.Value()->GetResponses();
                YT_VERIFY(std::ssize(responses) == totalDependentNodeCount);

                int batchSubrequestIndex = 0;
                for (const auto& operationRemoveRequest : requestsWithDependentNodesToRemove) {
                    bool removedAllDependentNodes = true;
                    for (auto dependentNodeId : operationRemoveRequest.DependentNodeIds) {
                        const auto& response = responses[batchSubrequestIndex++];

                        if (response.IsOK()) {
                            continue;
                        }

                        YT_LOG_DEBUG(
                            response,
                            "Failed to remove dependent node from Cypress (OperationId: %v, DependentNodeId: %v)",
                            operationRemoveRequest.Id,
                            dependentNodeId);
                        failedRequests.push_back(operationRemoveRequest);
                        removedAllDependentNodes = false;
                        break;
                    }

                    if (removedAllDependentNodes) {
                        ++allDependentNodesRemovedRequestCount;
                        requestsWithOperationNodeToRemove.push_back(operationRemoveRequest);
                    }
                }

                YT_LOG_DEBUG(
                    "Successfully removed dependent nodes from operations (OperationCount: %v, AllDependentNodesRemovedRequestCount: %v, FailedToRemoveAllDependentNodesRequestCount: %v)",
                    requestsWithDependentNodesToRemove.size(),
                    allDependentNodesRemovedRequestCount,
                    requestsWithDependentNodesToRemove.size() - allDependentNodesRemovedRequestCount);
            } else {
                YT_LOG_WARNING(
                    batchRspOrError,
                    "Failed to remove dependent nodes for operations from Cypress (OperationCount: %v)",
                    std::ssize(requestsWithDependentNodesToRemove));

                failedRequests.insert(
                    failedRequests.end(),
                    requestsWithDependentNodesToRemove.begin(),
                    requestsWithDependentNodesToRemove.end());

                failedToRemoveOperationCount += std::ssize(requestsWithDependentNodesToRemove);
            }
        }

        // Perform actual remove.
        if (!requestsWithOperationNodeToRemove.empty()) {
            int subbatchSize = Config_->RemoveSubbatchSize;

            YT_LOG_DEBUG(
                "Removing operation nodes from Cypress (OperationCount: %v, SubbatchSize: %v)",
                requestsWithOperationNodeToRemove.size(),
                subbatchSize);

            auto proxy = CreateObjectServiceWriteProxy(Client_);

            std::vector<TFuture<TObjectServiceProxy::TRspExecuteBatchPtr>> responseFutures;

            int subbatchCount = DivCeil(static_cast<int>(std::ssize(requestsWithOperationNodeToRemove)), subbatchSize);

            std::vector<int> subbatchSizes;
            for (int subbatchIndex = 0; subbatchIndex < subbatchCount; ++subbatchIndex) {
                auto batchReq = proxy.ExecuteBatch();

                int startIndex = subbatchIndex * subbatchSize;
                int endIndex = std::min(static_cast<int>(std::ssize(requestsWithOperationNodeToRemove)), startIndex + subbatchSize);
                for (int index = startIndex; index < endIndex; ++index) {
                    auto req = TYPathProxy::Remove(GetOperationPath(requestsWithOperationNodeToRemove[index].Id));
                    req->set_recursive(true);
                    batchReq->AddRequest(req, "remove_operation");
                }

                responseFutures.push_back(batchReq->Invoke());
            }

            auto responseResultsOrError = WaitFor(AllSet(responseFutures));
            YT_VERIFY(responseResultsOrError.IsOK());
            const auto& responseResults = responseResultsOrError.Value();

            for (int subbatchIndex = 0; subbatchIndex < subbatchCount; ++subbatchIndex) {
                int startIndex = subbatchIndex * subbatchSize;
                int endIndex = std::min(static_cast<int>(std::ssize(requestsWithOperationNodeToRemove)), startIndex + subbatchSize);

                const auto& batchRspOrError = responseResults[subbatchIndex];
                if (batchRspOrError.IsOK()) {
                    const auto& batchRsp = batchRspOrError.Value();
                    auto rsps = batchRsp->GetResponses<TYPathProxy::TRspRemove>("remove_operation");
                    YT_VERIFY(std::ssize(rsps) == endIndex - startIndex);

                    for (int index = startIndex; index < endIndex; ++index) {
                        auto removeRequest = requestsWithOperationNodeToRemove[index];

                        auto rsp = rsps[index - startIndex];
                        if (rsp.IsOK()) {
                            successfulRequests.push_back(removeRequest);
                        } else {
                            YT_LOG_DEBUG(
                                rsp,
                                "Failed to remove finished operation from Cypress (OperationId: %v)",
                                removeRequest.Id);

                            failedRequests.push_back(removeRequest);

                            ++failedToRemoveOperationCount;
                        }
                    }
                } else {
                    YT_LOG_WARNING(
                        batchRspOrError,
                        "Failed to remove finished operations from Cypress (OperationCount: %v)",
                        endIndex - startIndex);

                    for (int index = startIndex; index < endIndex; ++index) {
                        failedRequests.push_back(requestsWithOperationNodeToRemove[index]);
                        ++failedToRemoveOperationCount;
                    }
                }
            }
        }

        YT_VERIFY(std::ssize(requests) == std::ssize(failedRequests) + std::ssize(successfulRequests) + lockedOperationCount);
        int removedCount = std::ssize(successfulRequests);

        RemovedOperationCounter_.Increment(std::ssize(successfulRequests));
        RemoveOperationErrorCounter_.Increment(std::ssize(failedRequests) + lockedOperationCount);

        ProcessRemovedOperations(successfulRequests);

        for (auto operationId : failedRequests) {
            RemoveBatcher_->Enqueue(operationId);
        }

        RemovePendingLocked_ += lockedOperationCount;
        RemovePending_ -= removedCount;
        YT_LOG_DEBUG(
            "Successfully removed operations from Cypress (Count: %v, LockedCount: %v, FailedToRemoveCount: %v)",
            removedCount,
            lockedOperationCount,
            failedToRemoveOperationCount);
    }

    void RemoveOperations()
    {
        YT_ASSERT_INVOKER_AFFINITY(GetCancelableInvoker());

        auto batch = WaitFor(RemoveBatcher_->DequeueBatch())
            .ValueOrThrow();

        if (!batch.empty()) {
            DoRemoveOperations(std::move(batch));
        }

        auto callback = BIND(&TImpl::RemoveOperations, MakeStrong(this))
            .Via(GetCancelableInvoker());

        TDelayedExecutor::Submit(callback, RandomDuration(Config_->MaxRemovalSleepDelay));
    }

    void TemporarilyDisableArchivation()
    {
        YT_ASSERT_INVOKER_AFFINITY(GetCancelableInvoker());

        DoStopOperationArchivation();

        auto enableCallback = BIND(&TImpl::DoStartOperationArchivation, MakeStrong(this))
            .Via(GetCancelableInvoker());

        OperationArchivationStartCookie_ = TDelayedExecutor::Submit(
            enableCallback,
            Config_->ArchivationEnableDelay);

        auto enableTime = TInstant::Now() + Config_->ArchivationEnableDelay;

        SetSchedulerAlert(
            ESchedulerAlertType::OperationsArchivation,
            TError("Max enqueued operations limit reached; archivation is temporarily disabled")
            << TErrorAttribute("enable_time", enableTime));

        YT_LOG_INFO("Archivation is temporarily disabled (EnableTime: %v)", enableTime);
    }

    void FetchFinishedOperations()
    {
        try {
            DoFetchFinishedOperations();
        } catch (const std::exception& ex) {
            // NOTE(asaitgalin): Maybe disconnect? What can we do here?
            YT_LOG_WARNING(ex, "Failed to fetch finished operations from Cypress");
        }
    }

    void FetchBriefProgressFromArchive(std::vector<TArchiveOperationRequest>& requests)
    {
        auto idMapping = NRecords::TOrderedByIdDescriptor::Get()->GetIdMapping();
        std::vector<TOperationId> ids;
        ids.reserve(requests.size());
        for (const auto& req : requests) {
            ids.push_back(req.Id);
        }
        auto filter = TColumnFilter({*idMapping.BriefProgress});
        auto briefProgressIndex = filter.GetPosition(*idMapping.BriefProgress);
        auto timeout = Config_->FinishedOperationsArchiveLookupTimeout;
        auto rowsetOrError = LookupOperationsInArchive(Client_, ids, filter, timeout);
        if (!rowsetOrError.IsOK()) {
            YT_LOG_WARNING("Failed to fetch operation brief progress from archive (Error: %v)",
                rowsetOrError);
            return;
        }
        auto rows = rowsetOrError.Value()->GetRows();
        YT_VERIFY(rows.size() == requests.size());
        for (int i = 0; i < std::ssize(requests); ++i) {
            if (!requests[i].BriefProgress && rows[i] && rows[i][briefProgressIndex].Type != EValueType::Null) {
                auto value = rows[i][briefProgressIndex];
                requests[i].BriefProgress = TYsonString(value.AsString());
            }
        }
    }

    TObjectServiceProxy::TReqExecuteBatchPtr CreateBatchRequest()
    {
        auto proxy = CreateObjectServiceReadProxy(
            Client_,
            EMasterChannelKind::Follower);
        return proxy.ExecuteBatch();
    }

    std::vector<TArchiveOperationRequest> FetchOperationsFromCypressForCleaner(
        const std::vector<TOperationId>& operationIds)
    {
        using NYT::ToProto;

        struct TOperationDataToParse
        {
            TYsonString AttributesYson;
            TOperationId OperationId;
        };

        YT_LOG_INFO("Fetching operations attributes for cleaner (OperationCount: %v)", operationIds.size());

        std::vector<TArchiveOperationRequest> result;

        auto batchReq = CreateBatchRequest();

        for (auto operationId : operationIds) {
            auto req = TYPathProxy::Get(GetOperationPath(operationId) + "/@");
            ToProto(req->mutable_attributes()->mutable_keys(), TArchiveOperationRequest::GetAttributeKeys());
            batchReq->AddRequest(req, "get_op_attributes");
        }

        auto rspOrError = WaitFor(batchReq->Invoke());
        auto error = GetCumulativeError(rspOrError);
        if (!error.IsOK()) {
            THROW_ERROR_EXCEPTION("Error requesting operations attributes for archivation")
                << error;
        } else {
            YT_LOG_INFO("Fetched operations attributes for cleaner (OperationCount: %v)", operationIds.size());
        }

        auto rsps = rspOrError.Value()->GetResponses<TYPathProxy::TRspGet>("get_op_attributes");
        YT_VERIFY(operationIds.size() == rsps.size());

        auto parseOperationAttributesBatchSize = Config_->ParseOperationAttributesBatchSize;

        {
            const auto processBatch = BIND([this, this_ = MakeStrong(this), parseOperationAttributesBatchSize] (
                const std::vector<TOperationDataToParse>& operationDataToParseBatch)
            {
                std::vector<TArchiveOperationRequest> result;
                result.reserve(parseOperationAttributesBatchSize);

                for (const auto& operationDataToParse : operationDataToParseBatch) {
                    IAttributeDictionaryPtr attributes;
                    TOperationId operationId;
                    try {
                        attributes = ConvertToAttributes(operationDataToParse.AttributesYson);
                        operationId = TOperationId(TGuid::FromString(attributes->Get<TString>("key")));
                        YT_VERIFY(operationId == operationDataToParse.OperationId);
                    } catch (const std::exception& ex) {
                        THROW_ERROR_EXCEPTION("Error parsing operation attributes")
                            << TErrorAttribute("operation_id", operationDataToParse.OperationId)
                            << ex;
                    }

                    try {
                        result.push_back(InitializeRequestFromAttributes(*attributes));
                    } catch (const std::exception& ex) {
                        THROW_ERROR_EXCEPTION("Error initializing operation archivation request")
                            << TErrorAttribute("operation_id", operationId)
                            << TErrorAttribute("attributes", ConvertToYsonString(*attributes, EYsonFormat::Text))
                            << ex;
                    }
                }

                return result;
            });

            YT_LOG_INFO("Operations attributes for cleaner parsing started");

            int operationCount = std::ssize(operationIds);
            std::vector<TFuture<std::vector<TArchiveOperationRequest>>> futures;
            futures.reserve(RoundUp(operationCount, parseOperationAttributesBatchSize));

            for (int startIndex = 0; startIndex < operationCount; startIndex += parseOperationAttributesBatchSize) {
                std::vector<TOperationDataToParse> operationDataToParseBatch;
                operationDataToParseBatch.reserve(parseOperationAttributesBatchSize);

                for (int index = startIndex; index < std::min(operationCount, startIndex + parseOperationAttributesBatchSize); ++index) {
                    operationDataToParseBatch.push_back({TYsonString(rsps[index].Value()->value()), operationIds[index]});
                }

                futures.push_back(processBatch
                    .AsyncVia(Host_->GetBackgroundInvoker())
                    .Run(std::move(operationDataToParseBatch)));
            }

            auto operationRequestsArray = WaitFor(AllSucceeded(futures)).ValueOrThrow();

            result.reserve(operationCount);
            for (auto& operationRequests : operationRequestsArray) {
                for (auto& operationRequest : operationRequests) {
                    result.push_back(std::move(operationRequest));
                }
            }
        }

        YT_LOG_INFO("Operations attributes for cleaner fetched");

        return result;
    }

    void DoFetchFinishedOperations()
    {
        YT_LOG_INFO("Fetching all finished operations from Cypress");

        auto listOperationsResult = ListOperations(BIND(&TImpl::CreateBatchRequest, MakeStrong(this)));
        DoFetchFinishedOperationsById(std::move(listOperationsResult.OperationsToArchive));
    }

    void DoFetchFinishedOperationsById(std::vector<TOperationId> operationIds)
    {
        YT_LOG_INFO("Started fetching finished operations from Cypress (OperationCount: %v)", operationIds.size());
        auto operations = FetchOperationsFromCypressForCleaner(operationIds);

        // Controller agent reports brief_progress only to archive,
        // but it is necessary to fill ordered_by_start_time table,
        // so we request it here.
        FetchBriefProgressFromArchive(operations);

        // NB: Needed for us to store the latest operation for each alias in operation_aliases archive table.
        std::sort(operations.begin(), operations.end(), [] (const auto& lhs, const auto& rhs) {
            return lhs.FinishTime < rhs.FinishTime;
        });

        for (auto& operation : operations) {
            SubmitForArchivation(std::move(operation));
        }

        YT_LOG_INFO("Fetched and processed finished operations from Cypress (OperationCount: %v)", operationIds.size());
    }

    const TArchiveOperationRequest& GetRequest(TOperationId operationId) const
    {
        YT_ASSERT_INVOKER_AFFINITY(GetCancelableInvoker());

        return GetOrCrash(OperationMap_, operationId);
    }

    void ProcessRemovedOperations(const std::vector<TRemoveOperationRequest>& removedOperationRequests)
    {
        std::vector<TArchiveOperationRequest> removedOperationArchiveRequests;
        removedOperationArchiveRequests.reserve(removedOperationRequests.size());
        for (const auto& request : removedOperationRequests) {
            auto it = OperationMap_.find(request.Id);
            if (it != OperationMap_.end()) {
                removedOperationArchiveRequests.emplace_back(std::move(it->second));
                OperationMap_.erase(it);
            }
        }

        OperationsRemovedFromCypress_.Fire(removedOperationArchiveRequests);
    }

    void SendOperationAlerts()
    {
        YT_ASSERT_INVOKER_AFFINITY(GetCancelableInvoker());

        if (ArchiveVersion_ < 43 || OperationAlertEventQueue_.empty()) {
            SetSchedulerAlert(ESchedulerAlertType::OperationAlertArchivation, TError());
            return;
        }

        std::deque<TOperationAlertEvent> eventsToSend;
        eventsToSend.swap(OperationAlertEventQueue_);
        try {
            WaitFor(BIND([
                    client = Client_,
                    eventsToSend,
                    maxAlertEventCountPerAlertType = Config_->MaxAlertEventCountPerAlertType,
                    tabletTransaction = Config_->TabletTransactionTimeout] () mutable {
                    NDetail::DoSendOperationAlerts(
                        std::move(client),
                        std::move(eventsToSend),
                        maxAlertEventCountPerAlertType,
                        tabletTransaction);
                })
                .AsyncVia(Host_->GetBackgroundInvoker())
                .Run())
                .ThrowOnError();
            LastOperationAlertEventSendTime_ = TInstant::Now();
            SetSchedulerAlert(ESchedulerAlertType::OperationAlertArchivation, TError());
            ArchivedOperationAlertEventCounter_.Increment(eventsToSend.size());
        } catch (const std::exception& ex) {
            auto error = TError("Failed to write operation alert events to archive")
                << ex;
            YT_LOG_WARNING(error);
            if (TInstant::Now() - LastOperationAlertEventSendTime_ > Config_->OperationAlertSenderAlertThreshold) {
                SetSchedulerAlert(ESchedulerAlertType::OperationAlertArchivation, error);
            }

            while (!eventsToSend.empty() && std::ssize(OperationAlertEventQueue_) < Config_->MaxEnqueuedOperationAlertEventCount) {
                OperationAlertEventQueue_.emplace_front(std::move(eventsToSend.back()));
                eventsToSend.pop_back();
            }

            if (!eventsToSend.empty()) {
                YT_LOG_WARNING("Some alerts have been dropped due to alert event queue overflow (DroppedEventCount: %v)",
                    std::ssize(eventsToSend));
            }
            DroppedOperationAlertEventCounter_.Increment(std::ssize(eventsToSend));
        }
        EnqueuedAlertEvents_.store(std::ssize(OperationAlertEventQueue_));
    }

    void CheckAndTruncateAlertEvents()
    {
        i64 droppedEventCount = 0;
        while (std::ssize(OperationAlertEventQueue_) > Config_->MaxEnqueuedOperationAlertEventCount) {
            OperationAlertEventQueue_.pop_front();
            ++droppedEventCount;
        }
        EnqueuedAlertEvents_.store(std::ssize(OperationAlertEventQueue_));
        DroppedOperationAlertEventCounter_.Increment(droppedEventCount);
    }

    void ProcessWaitingLockedOperations()
    {
        auto now = TInstant::Now();
        while (!LockedOperationQueue_.empty()) {
            const auto& [operationId, enqueueInstant] = LockedOperationQueue_.front();
            if (enqueueInstant + Config_->LockedOperationWaitTimeout > now) {
                break;
            }
            RemoveBatcher_->Enqueue(operationId);
            LockedOperationQueue_.pop_front();
            --RemovePendingLocked_;
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

TOperationsCleaner::TOperationsCleaner(
    TOperationsCleanerConfigPtr config,
    IOperationsCleanerHost* host,
    TBootstrap* bootstrap)
    : Impl_(New<TImpl>(std::move(config), host, bootstrap))
{ }

TOperationsCleaner::~TOperationsCleaner()
{ }

void TOperationsCleaner::Start()
{
    Impl_->Start();
}

void TOperationsCleaner::Stop()
{
    Impl_->Stop();
}

void TOperationsCleaner::SubmitForArchivation(TArchiveOperationRequest request)
{
    Impl_->SubmitForArchivation(std::move(request));
}

void TOperationsCleaner::SubmitForArchivation(std::vector<TOperationId> operationIds)
{
    Impl_->SubmitForArchivation(std::move(operationIds));
}

void TOperationsCleaner::SubmitForRemoval(std::vector<TRemoveOperationRequest> requests)
{
    Impl_->SubmitForRemoval(std::move(requests));
}

void TOperationsCleaner::UpdateConfig(const TOperationsCleanerConfigPtr& config)
{
    Impl_->UpdateConfig(config);
}

void TOperationsCleaner::SetArchiveVersion(int version)
{
    Impl_->SetArchiveVersion(version);
}

bool TOperationsCleaner::IsEnabled() const
{
    return Impl_->IsEnabled();
}

void TOperationsCleaner::BuildOrchid(TFluentMap fluent) const
{
    Impl_->BuildOrchid(fluent);
}

void TOperationsCleaner::EnqueueOperationAlertEvent(
    TOperationId operationId,
    EOperationAlertType alertType,
    TError alert)
{
    Impl_->EnqueueOperationAlertEvent(operationId, alertType, std::move(alert));
}

DELEGATE_SIGNAL(TOperationsCleaner, void(const std::vector<TArchiveOperationRequest>& requests), OperationsRemovedFromCypress, *Impl_);

TArchiveOperationRequest TOperationsCleaner::InitializeRequestFromOperation(const TOperationPtr& operation)
{
    return Impl_->InitializeRequestFromOperation(operation);
}

TArchiveOperationRequest TOperationsCleaner::InitializeRequestFromAttributes(const IAttributeDictionary& attributes)
{
    return Impl_->InitializeRequestFromAttributes(attributes);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
