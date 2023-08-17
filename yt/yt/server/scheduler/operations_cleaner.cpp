#include "private.h"
#include "operations_cleaner.h"
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

#include <yt/yt/client/api/rowset.h>
#include <yt/yt/client/api/transaction.h>
#include <yt/yt/client/api/operation_archive_schema.h>

#include <yt/yt/client/security_client/public.h>

#include <yt/yt/client/table_client/row_buffer.h>
#include <yt/yt/client/table_client/helpers.h>
#include <yt/yt/client/table_client/record_helpers.h>

#include <yt/yt/core/actions/cancelable_context.h>

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/nonblocking_batch.h>
#include <yt/yt/core/concurrency/periodic_executor.h>
#include <yt/yt/core/concurrency/async_semaphore.h>

#include <yt/yt/core/misc/numeric_helpers.h>

#include <yt/yt/core/rpc/dispatcher.h>

#include <yt/yt/core/utilex/random.h>

#include <yt/yt/core/yson/string_filter.h>

#include <yt/yt/core/ytree/fluent.h>
#include <yt/yt/core/ytree/ypath_resolver.h>

namespace NYT::NScheduler {

using namespace NConcurrency;
using namespace NYTree;
using namespace NYson;
using namespace NObjectClient;
using namespace NApi;
using namespace NTableClient;
using namespace NTransactionClient;
using namespace NProfiling;

////////////////////////////////////////////////////////////////////////////////

static inline const NLogging::TLogger Logger("OperationsCleaner");

////////////////////////////////////////////////////////////////////////////////

struct TOrderedByIdTag
{ };

struct TOrderedByStartTimeTag
{ };

struct TOperationAliasesTag
{ };

////////////////////////////////////////////////////////////////////////////////

void TArchiveOperationRequest::InitializeFromOperation(const TOperationPtr& operation)
{
    Id = operation->GetId();
    StartTime = operation->GetStartTime();
    FinishTime = *operation->GetFinishTime();
    State = operation->GetState();
    AuthenticatedUser = operation->GetAuthenticatedUser();
    OperationType = operation->GetType();
    Spec = operation->GetSpecString();
    Result = operation->BuildResultString();
    Events = ConvertToYsonString(operation->Events());
    Alerts = operation->BuildAlertsString();
    BriefSpec = operation->BriefSpecString();
    RuntimeParameters = ConvertToYsonString(operation->GetRuntimeParameters(), EYsonFormat::Binary);
    Alias = operation->Alias();
    SlotIndexPerPoolTree = ConvertToYsonString(operation->GetSlotIndices(), EYsonFormat::Binary);
    TaskNames = ConvertToYsonString(operation->GetTaskNames(), EYsonFormat::Binary);
    ExperimentAssignments = ConvertToYsonString(operation->ExperimentAssignments(), EYsonFormat::Binary);
    ExperimentAssignmentNames = ConvertToYsonString(operation->GetExperimentAssignmentNames(), EYsonFormat::Binary);
    ProvidedSpec = operation->ProvidedSpecString();

    const auto& attributes = operation->ControllerAttributes();
    const auto& initializationAttributes = attributes.InitializeAttributes;
    if (initializationAttributes) {
        UnrecognizedSpec = initializationAttributes->UnrecognizedSpec;
        FullSpec = initializationAttributes->FullSpec;
    }
}

const std::vector<TString>& TArchiveOperationRequest::GetAttributeKeys()
{
    // Keep the stuff below synchronized with InitializeFromAttributes method.
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
        "slot_index_per_pool_tree",
        "task_names",
        "experiment_assignments",
        "controller_features",
        "provided_spec",
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

void TArchiveOperationRequest::InitializeFromAttributes(const IAttributeDictionary& attributes)
{
    Id = TOperationId::FromString(attributes.Get<TString>("key"));
    StartTime = attributes.Get<TInstant>("start_time");
    FinishTime = attributes.Get<TInstant>("finish_time");
    State = attributes.Get<EOperationState>("state");
    AuthenticatedUser = attributes.Get<TString>("authenticated_user");
    OperationType = attributes.Get<EOperationType>("operation_type");
    Progress = attributes.FindYson("progress");
    BriefProgress = attributes.FindYson("brief_progress");
    Spec = attributes.GetYson("spec");
    // In order to recover experiment assignment names, we must either
    // dig into assignment YSON representation or reconstruct assignment objects.
    // The latter seems more convenient. Also, do not forget that older operations
    // may miss assignment attribute at all.
    if (auto experimentAssignmentsYson = attributes.FindYson("experiment_assignments")) {
        ExperimentAssignments = experimentAssignmentsYson;
        auto experimentAssignments = ConvertTo<std::vector<TExperimentAssignmentPtr>>(experimentAssignmentsYson);
        std::vector<TString> experimentAssignmentNames;
        experimentAssignmentNames.reserve(experimentAssignments.size());
        for (const auto& experimentAssignment : experimentAssignments) {
            experimentAssignmentNames.emplace_back(experimentAssignment->GetName());
        }
        ExperimentAssignmentNames = ConvertToYsonString(experimentAssignmentNames, EYsonFormat::Binary);
    }

    ProvidedSpec = attributes.FindYson("provided_spec");
    BriefSpec = attributes.FindYson("brief_spec");
    Result = attributes.GetYson("result");
    Events = attributes.GetYson("events");
    Alerts = attributes.GetYson("alerts");
    FullSpec = attributes.FindYson("full_spec");
    UnrecognizedSpec = attributes.FindYson("unrecognized_spec");

    if (auto heavyRuntimeParameters = attributes.Find<IMapNodePtr>("heavy_runtime_parameters")) {
        auto runtimeParameters = attributes.Find<IMapNodePtr>("runtime_parameters");
        if (!runtimeParameters) {
            RuntimeParameters = ConvertToYsonString(heavyRuntimeParameters);
        } else {
            RuntimeParameters = ConvertToYsonString(PatchNode(runtimeParameters, heavyRuntimeParameters));
        }
    } else {
        RuntimeParameters = attributes.FindYson("runtime_parameters");
    }
    Alias = ConvertTo<TOperationSpecBasePtr>(Spec)->Alias;
    SlotIndexPerPoolTree = attributes.FindYson("slot_index_per_pool_tree");
    TaskNames = attributes.FindYson("task_names");
    ControllerFeatures = attributes.FindYson("controller_features");
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
    auto getPaths = [] () {
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

struct TAnnotationsAndScheduligOptions
{
    INodePtr Annotations;
    INodePtr SchedulingOptionsPerPoolTree;

    static const inline std::vector<std::pair<TString, INodePtr TAnnotationsAndScheduligOptions::*>> Fields = {
            {"annotations", &TAnnotationsAndScheduligOptions::Annotations},
            {"scheduling_options_per_pool_tree", &TAnnotationsAndScheduligOptions::SchedulingOptionsPerPoolTree},
    };
};

struct TAclAndScheduligOptions
{
    INodePtr Acl;
    INodePtr SchedulingOptionsPerPoolTree;

    static const inline std::vector<std::pair<TString, INodePtr TAclAndScheduligOptions::*>> Fields = {
            {"acl", &TAclAndScheduligOptions::Acl},
            {"scheduling_options_per_pool_tree", &TAclAndScheduligOptions::SchedulingOptionsPerPoolTree},
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

TString GetFilterFactors(const TArchiveOperationRequest& request)
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

    auto filteredRuntimeParameters = FilterYsonAndLoadStruct<TAnnotationsAndScheduligOptions>(request.RuntimeParameters);
    auto filteredSpec = FilterYsonAndLoadStruct<TFilteredSpecAttributes>(request.Spec);

    std::vector<TString> parts;
    parts.push_back(ToString(request.Id));
    parts.push_back(request.AuthenticatedUser);
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

TUnversionedRow BuildOrderedByIdTableRow(
    const TRowBufferPtr& rowBuffer,
    const TArchiveOperationRequest& request,
    const TOrderedByIdTableDescriptor::TIndex& index,
    int version)
{
    // All any and string values passed to MakeUnversioned* functions MUST be alive till
    // they are captured in row buffer (they are not owned by unversioned value or builder).
    auto state = FormatEnum(request.State);
    auto operationType = FormatEnum(request.OperationType);
    auto filterFactors = GetFilterFactors(request);

    TUnversionedRowBuilder builder;
    builder.AddValue(MakeUnversionedUint64Value(request.Id.Parts64[0], index.IdHi));
    builder.AddValue(MakeUnversionedUint64Value(request.Id.Parts64[1], index.IdLo));
    builder.AddValue(MakeUnversionedStringValue(state, index.State));
    builder.AddValue(MakeUnversionedStringValue(request.AuthenticatedUser, index.AuthenticatedUser));
    builder.AddValue(MakeUnversionedStringValue(operationType, index.OperationType));
    if (request.Progress && NeedProgressInRequest(request.Progress)) {
        builder.AddValue(MakeUnversionedAnyValue(request.Progress.AsStringBuf(), index.Progress));
    }
    if (request.BriefProgress && NeedProgressInRequest(request.BriefProgress)) {
        builder.AddValue(MakeUnversionedAnyValue(request.BriefProgress.AsStringBuf(), index.BriefProgress));
    }
    builder.AddValue(MakeUnversionedAnyValue(request.Spec.AsStringBuf(), index.Spec));
    if (request.BriefSpec) {
        builder.AddValue(MakeUnversionedAnyValue(request.BriefSpec.AsStringBuf(), index.BriefSpec));
    }
    builder.AddValue(MakeUnversionedInt64Value(request.StartTime.MicroSeconds(), index.StartTime));
    builder.AddValue(MakeUnversionedInt64Value(request.FinishTime.MicroSeconds(), index.FinishTime));
    builder.AddValue(MakeUnversionedStringValue(filterFactors, index.FilterFactors));
    builder.AddValue(MakeUnversionedAnyValue(request.Result.AsStringBuf(), index.Result));
    builder.AddValue(MakeUnversionedAnyValue(request.Events.AsStringBuf(), index.Events));
    if (request.Alerts) {
        builder.AddValue(MakeUnversionedAnyValue(request.Alerts.AsStringBuf(), index.Alerts));
    }
    if (request.UnrecognizedSpec) {
        builder.AddValue(MakeUnversionedAnyValue(request.UnrecognizedSpec.AsStringBuf(), index.UnrecognizedSpec));
    }
    if (request.FullSpec) {
        builder.AddValue(MakeUnversionedAnyValue(request.FullSpec.AsStringBuf(), index.FullSpec));
    }

    if (request.RuntimeParameters) {
        builder.AddValue(MakeUnversionedAnyValue(request.RuntimeParameters.AsStringBuf(), index.RuntimeParameters));
    }

    if (request.SlotIndexPerPoolTree) {
        builder.AddValue(MakeUnversionedAnyValue(request.SlotIndexPerPoolTree.AsStringBuf(), index.SlotIndexPerPoolTree));
    }

    if (request.TaskNames) {
        builder.AddValue(MakeUnversionedAnyValue(request.TaskNames.AsStringBuf(), index.TaskNames));
    }

    if (version >= 40 && request.ExperimentAssignments) {
        builder.AddValue(MakeUnversionedAnyValue(request.ExperimentAssignments.AsStringBuf(), index.ExperimentAssignments));
        builder.AddValue(MakeUnversionedAnyValue(request.ExperimentAssignmentNames.AsStringBuf(), index.ExperimentAssignmentNames));
    }

    if (version >= 42 && request.ControllerFeatures) {
        builder.AddValue(MakeUnversionedAnyValue(request.ControllerFeatures.AsStringBuf(), index.ControllerFeatures));
    }

    if (version >= 46 && request.ProvidedSpec) {
        builder.AddValue(MakeUnversionedAnyValue(request.ProvidedSpec.AsStringBuf(), index.ProvidedSpec));
    }

    return rowBuffer->CaptureRow(builder.GetRow());
}

TUnversionedRow BuildOrderedByStartTimeTableRow(
    const TRowBufferPtr& rowBuffer,
    const TArchiveOperationRequest& request,
    const TOrderedByStartTimeTableDescriptor::TIndex& index,
    int version)
{
    // All any and string values passed to MakeUnversioned* functions MUST be alive till
    // they are captured in row buffer (they are not owned by unversioned value or builder).
    auto state = FormatEnum(request.State);
    auto operationType = FormatEnum(request.OperationType);
    auto filterFactors = GetFilterFactors(request);

    TYsonString pools;
    TYsonString poolTreeToPool;
    TYsonString acl;

    if (request.RuntimeParameters) {
        auto filteredRuntimeParameters = FilterYsonAndLoadStruct<TAclAndScheduligOptions>(request.RuntimeParameters);
        pools = ConvertToYsonString(GetPools(filteredRuntimeParameters.SchedulingOptionsPerPoolTree));
        poolTreeToPool = ConvertToYsonString(GetPoolTreeToPool(filteredRuntimeParameters.SchedulingOptionsPerPoolTree));
        if (filteredRuntimeParameters.Acl) {
            acl = ConvertToYsonString(filteredRuntimeParameters.Acl);
        }
    }

    TUnversionedRowBuilder builder;
    builder.AddValue(MakeUnversionedInt64Value(request.StartTime.MicroSeconds(), index.StartTime));
    builder.AddValue(MakeUnversionedUint64Value(request.Id.Parts64[0], index.IdHi));
    builder.AddValue(MakeUnversionedUint64Value(request.Id.Parts64[1], index.IdLo));
    builder.AddValue(MakeUnversionedStringValue(operationType, index.OperationType));
    builder.AddValue(MakeUnversionedStringValue(state, index.State));
    builder.AddValue(MakeUnversionedStringValue(request.AuthenticatedUser, index.AuthenticatedUser));
    builder.AddValue(MakeUnversionedStringValue(filterFactors, index.FilterFactors));

    if (pools) {
        builder.AddValue(MakeUnversionedAnyValue(pools.AsStringBuf(), index.Pools));
    }
    if (request.BriefProgress) {
        builder.AddValue(MakeUnversionedBooleanValue(HasFailedJobs(request.BriefProgress), index.HasFailedJobs));
    }

    if (acl) {
        builder.AddValue(MakeUnversionedAnyValue(acl.AsStringBuf(), index.Acl));
    }

    if (version >= 44 && poolTreeToPool) {
        builder.AddValue(MakeUnversionedAnyValue(poolTreeToPool.AsStringBuf(), index.PoolTreeToPool));
    }

    return rowBuffer->CaptureRow(builder.GetRow());
}

TUnversionedRow BuildOperationAliasesTableRow(
    const TRowBufferPtr& rowBuffer,
    const TArchiveOperationRequest& request,
    int /*version*/)
{
    NRecords::TOperationAlias record{
        .Key = {
            .Alias =  *request.Alias,
        },
        .OperationIdHi = request.Id.Parts64[0],
        .OperationIdLo = request.Id.Parts64[1],
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

    TOrderedByIdTableDescriptor tableDescriptor;
    const auto& tableIndex = tableDescriptor.Index;
    auto columns = std::vector{tableIndex.IdHi, tableIndex.IdLo, tableIndex.AlertEvents};
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

    auto idHiIndex = columnFilter.GetPosition(tableIndex.IdHi);
    auto idLoIndex = columnFilter.GetPosition(tableIndex.IdLo);
    auto alertEventsIndex = columnFilter.GetPosition(tableIndex.AlertEvents);

    THashMap<TOperationId, TAlertEventsMap> idToAlertEvents;
    for (auto row : rowset->GetRows()) {
        if (!row) {
            continue;
        }
        auto operationId = TOperationId(
            FromUnversionedValue<ui64>(row[idHiIndex]),
            FromUnversionedValue<ui64>(row[idLoIndex]));

        auto eventsFromArchive = FromUnversionedValue<std::optional<TYsonStringBuf>>(row[alertEventsIndex]);
        if (eventsFromArchive) {
            idToAlertEvents.emplace(
                operationId,
                ConvertToAlertEventsMap(ConvertTo<std::vector<TOperationAlertEvent>>(*eventsFromArchive)));
        }
    }
    for (const auto& alertEvent : eventsToSend) {
        // Id can be absent in idToAlertEvents if row with such id is not created in archive yet.
        // In this case we want to create this row and initialize it with empty operation alert history.
        YT_VERIFY(alertEvent.OperationId);
        AddEventToAlertEventsMap(&idToAlertEvents[*alertEvent.OperationId], alertEvent, maxAlertEventCountPerAlertType);
    }

    auto rowBuffer = New<TRowBuffer>();
    std::vector<TUnversionedRow> rows;
    rows.reserve(idToAlertEvents.size());

    for (const auto& [operationId, eventsMap] : idToAlertEvents) {
        TUnversionedRowBuilder builder;
        builder.AddValue(MakeUnversionedUint64Value(operationId.Parts64[0], tableIndex.IdHi));
        builder.AddValue(MakeUnversionedUint64Value(operationId.Parts64[1], tableIndex.IdLo));
        auto serializedEvents = ConvertToYsonString(ConvertToAlertEvents(eventsMap));
        builder.AddValue(MakeUnversionedAnyValue(serializedEvents.AsStringBuf(), tableIndex.AlertEvents));

        rows.push_back(rowBuffer->CaptureRow(builder.GetRow()));
    }

    TTransactionStartOptions options;
    options.Timeout = transactionTimeout;

    auto transaction = WaitFor(client->StartTransaction(ETransactionType::Tablet, options))
        .ValueOrThrow();
    transaction->WriteRows(
        GetOperationsArchiveOrderedByIdPath(),
        tableDescriptor.NameTable,
        MakeSharedRange(std::move(rows), std::move(rowBuffer)));

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
        , RemoveBatcher_(New<TNonblockingBatch<TOperationId>>(
            Config_->RemoveBatchSize,
            Config_->RemoveBatchTimeout))
        , ArchiveBatcher_(New<TNonblockingBatch<TOperationId>>(
            Config_->ArchiveBatchSize,
            Config_->ArchiveBatchTimeout))
        , Client_(Bootstrap_->GetClient()->GetNativeConnection()
            ->CreateNativeClient(TClientOptions::FromUser(NSecurityClient::OperationsCleanerUserName)))
    {
        Profiler.WithTag("locked", "true").AddFuncGauge("/remove_pending", MakeStrong(this), [this] {
            return RemovePendingLocked_.load();
        });
        Profiler.WithTag("locked", "false").AddFuncGauge("/remove_pending", MakeStrong(this), [this] {
            return RemovePending_.load() - RemovePendingLocked_.load();
        });
        Profiler.AddFuncGauge("/archive_pending", MakeStrong(this), [this] {
            return ArchivePending_.load();
        });
        Profiler.AddFuncGauge("/submitted", MakeStrong(this), [this] {
            return Submitted_.load();
        });
        Profiler.AddFuncGauge("/alert_events/enqueued", MakeStrong(this), [this] {
            return EnqueuedAlertEvents_.load();
        });
    }

    void Start()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        GetUncancelableInvoker()->Invoke(BIND(&TImpl::DoStart, MakeStrong(this), /*fetchFinishedOperations*/ false));
    }

    void Stop()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        GetUncancelableInvoker()->Invoke(BIND(&TImpl::DoStop, MakeStrong(this)));
    }

    void UpdateConfig(const TOperationsCleanerConfigPtr& config)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

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

    void SubmitForRemoval(std::vector<TOperationId> operationIds)
    {
        if (!IsEnabled()) {
            return;
        }

        GetCancelableInvoker()->Invoke(BIND([this, this_ = MakeStrong(this), operationIds = std::move(operationIds)] {
            for (auto operationId : operationIds) {
                EnqueueForRemoval(operationId);
            }
        }));
    }

    void SetArchiveVersion(int version)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        ArchiveVersion_ = version;
    }

    bool IsEnabled() const
    {
        return Enabled_;
    }

    void BuildOrchid(TFluentMap fluent) const
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

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
        VERIFY_THREAD_AFFINITY(ControlThread);

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

    TIntrusivePtr<TNonblockingBatch<TOperationId>> RemoveBatcher_;
    TIntrusivePtr<TNonblockingBatch<TOperationId>> ArchiveBatcher_;
    std::deque<std::pair<TOperationId, TInstant>> LockedOperationQueue_;

    std::deque<TOperationAlertEvent> OperationAlertEventQueue_;
    TInstant LastOperationAlertEventSendTime_;

    NNative::IClientPtr Client_;

    TProfiler Profiler{"/operations_cleaner"};
    std::atomic<i64> RemovePending_{0};
    std::atomic<i64> ArchivePending_{0};
    std::atomic<i64> Submitted_{0};
    std::atomic<i64> EnqueuedAlertEvents_{0};
    std::atomic<i64> RemovePendingLocked_{0};

    TCounter ArchivedOperationCounter_ = Profiler.Counter("/archived");
    TCounter RemovedOperationCounter_ = Profiler.Counter("/removed");
    TCounter CommittedDataWeightCounter_ = Profiler.Counter("/committed_data_weight");
    TCounter ArchiveErrorCounter_ = Profiler.Counter("/archive_errors");
    TCounter RemoveOperationErrorCounter_ = Profiler.Counter("/remove_errors");
    TCounter ArchivedOperationAlertEventCounter_ = Profiler.Counter("/alert_events/archived");
    TEventTimer AnalyzeOperationsTimer_ = Profiler.Timer("/analyze_operations_time");
    TEventTimer OperationsRowsPreparationTimer_ = Profiler.Timer("/operations_rows_preparation_time");

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
        VERIFY_INVOKER_AFFINITY(GetUncancelableInvoker());

        Bootstrap_->GetControlInvoker(EControlQueue::OperationsCleaner)->Invoke(
            BIND(&IOperationsCleanerHost::SetSchedulerAlert, Host_, alertType, alert));
    }

    void DoStart(bool fetchFinishedOperations)
    {
        VERIFY_INVOKER_AFFINITY(GetUncancelableInvoker());

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
        VERIFY_INVOKER_AFFINITY(GetUncancelableInvoker());

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
        VERIFY_INVOKER_AFFINITY(GetUncancelableInvoker());

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

        ArchiveBatcher_->UpdateMaxBatchSize(Config_->ArchiveBatchSize);
        ArchiveBatcher_->UpdateBatchDuration(Config_->ArchiveBatchTimeout);

        RemoveBatcher_->UpdateMaxBatchSize(Config_->RemoveBatchSize);
        RemoveBatcher_->UpdateBatchDuration(Config_->RemoveBatchTimeout);

        YT_LOG_INFO("Operations cleaner config updated (Enable: %v, EnableOperationArchivation: %v, EnableOperationAlertEventArchivation: %v)",
            Config_->Enable,
            Config_->EnableOperationArchivation,
            Config_->EnableOperationAlertEventArchivation);
    }

    void DoSubmitForArchivation(TArchiveOperationRequest request)
    {
        VERIFY_INVOKER_AFFINITY(GetCancelableInvoker());

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
        VERIFY_INVOKER_AFFINITY(GetCancelableInvoker());

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

    void EnqueueForRemoval(TOperationId operationId)
    {
        VERIFY_INVOKER_AFFINITY(GetUncancelableInvoker());

        YT_LOG_DEBUG("Operation enqueued for removal (OperationId: %v)", operationId);
        RemovePending_++;
        RemoveBatcher_->Enqueue(operationId);
    }

    void EnqueueForArchivation(TOperationId operationId)
    {
        VERIFY_INVOKER_AFFINITY(GetCancelableInvoker());

        YT_LOG_DEBUG("Operation enqueued for archivation (OperationId: %v)", operationId);
        ArchivePending_++;
        ArchiveBatcher_->Enqueue(operationId);
    }

    void CleanOperation(TOperationId operationId)
    {
        VERIFY_INVOKER_AFFINITY(GetCancelableInvoker());

        if (IsOperationArchivationEnabled()) {
            EnqueueForArchivation(operationId);
        } else {
            EnqueueForRemoval(operationId);
        }
    }

    void TryArchiveOperations(const std::vector<TOperationId>& operationIds)
    {
        VERIFY_INVOKER_AFFINITY(GetCancelableInvoker());

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
                TOrderedByIdTableDescriptor desc;
                auto rowBuffer = New<TRowBuffer>(TOrderedByIdTag{});
                std::vector<TUnversionedRow> rows;
                rows.reserve(operationIds.size());

                for (auto operationId : operationIds) {
                    try {
                        const auto& request = GetRequest(operationId);
                        auto row = NDetail::BuildOrderedByIdTableRow(rowBuffer, request, desc.Index, version);

                        if (isValueWeightViolated(row, operationId, desc.NameTable)) {
                            skippedOperationIds.insert(operationId);
                            continue;
                        }

                        rows.push_back(row);
                        orderedByIdRowsDataWeight += GetDataWeight(row);
                    } catch (const std::exception& ex) {
                        THROW_ERROR_EXCEPTION("Failed to build row for operation %v", operationId)
                            << ex;
                    }
                }

                transaction->WriteRows(
                    GetOperationsArchiveOrderedByIdPath(),
                    desc.NameTable,
                    MakeSharedRange(std::move(rows), std::move(rowBuffer)));
            }

            // ordered_by_start_time rows
            {
                TOrderedByStartTimeTableDescriptor desc;
                auto rowBuffer = New<TRowBuffer>(TOrderedByStartTimeTag{});
                std::vector<TUnversionedRow> rows;
                rows.reserve(operationIds.size());

                for (auto operationId : operationIds) {
                    if (skippedOperationIds.contains(operationId)) {
                        continue;
                    }
                    try {
                        const auto& request = GetRequest(operationId);
                        auto row = NDetail::BuildOrderedByStartTimeTableRow(rowBuffer, request, desc.Index, version);
                        rows.push_back(row);
                        orderedByStartTimeRowsDataWeight += GetDataWeight(row);
                    } catch (const std::exception& ex) {
                        THROW_ERROR_EXCEPTION("Failed to build row for operation %v", operationId)
                            << ex;
                    }
                }

                transaction->WriteRows(
                    GetOperationsArchiveOrderedByStartTimePath(),
                    desc.NameTable,
                    MakeSharedRange(std::move(rows), std::move(rowBuffer)));
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
        VERIFY_INVOKER_AFFINITY(GetCancelableInvoker());

        auto batch = WaitFor(ArchiveBatcher_->DequeueBatch())
            .ValueOrThrow();

        if (!batch.empty()) {
            while (IsOperationArchivationEnabled()) {
                TError error;
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
                EnqueueForRemoval(operationId);
            }

            ArchivePending_ -= batch.size();
        }

        ScheduleArchiveOperations();
    }

    void DoRemoveOperations(std::vector<TOperationId> operationIds)
    {
        YT_LOG_DEBUG("Removing operations from Cypress (OperationCount: %v)", operationIds.size());

        ProcessWaitingLockedOperations();

        std::vector<TOperationId> failedOperationIds;
        std::vector<TOperationId> removedOperationIds;
        std::vector<TOperationId> operationIdsToRemove;

        int lockedOperationCount = 0;
        int failedToRemoveOperationCount = 0;

        // Fetch lock_count attribute.
        {
            auto proxy = CreateObjectServiceReadProxy(
                Client_,
                EMasterChannelKind::Follower);
            auto batchReq = proxy.ExecuteBatch();

            for (auto operationId : operationIds) {
                auto req = TYPathProxy::Get(GetOperationPath(operationId) + "/@lock_count");
                batchReq->AddRequest(req, "get_lock_count");
            }

            auto batchRspOrError = WaitFor(batchReq->Invoke());

            if (batchRspOrError.IsOK()) {
                const auto& batchRsp = batchRspOrError.Value();
                auto rsps = batchRsp->GetResponses<TYPathProxy::TRspGet>("get_lock_count");
                YT_VERIFY(std::ssize(rsps) == std::ssize(operationIds));

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

                    auto operationId = operationIds[index];
                    if (isLocked) {
                        LockedOperationQueue_.emplace_back(std::move(operationId), now);
                        ++lockedOperationCount;
                    } else {
                        operationIdsToRemove.push_back(operationId);
                    }
                }
            } else {
                YT_LOG_WARNING(
                    batchRspOrError,
                    "Failed to get lock count for operations from Cypress (OperationCount: %v)",
                    operationIds.size());

                failedOperationIds = operationIds;

                failedToRemoveOperationCount = std::ssize(operationIds);
            }
        }

        // Perform actual remove.
        if (!operationIdsToRemove.empty()) {
            int subbatchSize = Config_->RemoveSubbatchSize;

            auto proxy = CreateObjectServiceWriteProxy(Client_);

            std::vector<TFuture<TObjectServiceProxy::TRspExecuteBatchPtr>> responseFutures;

            int subbatchCount = DivCeil(static_cast<int>(std::ssize(operationIdsToRemove)), subbatchSize);

            std::vector<int> subbatchSizes;
            for (int subbatchIndex = 0; subbatchIndex < subbatchCount; ++subbatchIndex) {
                auto batchReq = proxy.ExecuteBatch();

                int startIndex = subbatchIndex * subbatchSize;
                int endIndex = std::min(static_cast<int>(std::ssize(operationIdsToRemove)), startIndex + subbatchSize);
                for (int index = startIndex; index < endIndex; ++index) {
                    auto req = TYPathProxy::Remove(GetOperationPath(operationIdsToRemove[index]));
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
                int endIndex = std::min(static_cast<int>(std::ssize(operationIdsToRemove)), startIndex + subbatchSize);

                const auto& batchRspOrError = responseResults[subbatchIndex];
                if (batchRspOrError.IsOK()) {
                    const auto& batchRsp = batchRspOrError.Value();
                    auto rsps = batchRsp->GetResponses<TYPathProxy::TRspRemove>("remove_operation");
                    YT_VERIFY(std::ssize(rsps) == endIndex - startIndex);

                    for (int index = startIndex; index < endIndex; ++index) {
                        auto operationId = operationIdsToRemove[index];

                        auto rsp = rsps[index - startIndex];
                        if (rsp.IsOK()) {
                            removedOperationIds.push_back(operationId);
                        } else {
                            YT_LOG_DEBUG(
                                rsp,
                                "Failed to remove finished operation from Cypress (OperationId: %v)",
                                operationId);

                            failedOperationIds.push_back(operationId);

                            ++failedToRemoveOperationCount;
                        }
                    }
                } else {
                    YT_LOG_WARNING(
                        batchRspOrError,
                        "Failed to remove finished operations from Cypress (OperationCount: %v)",
                        endIndex - startIndex);

                    for (int index = startIndex; index < endIndex; ++index) {
                        failedOperationIds.push_back(operationIdsToRemove[index]);
                        ++failedToRemoveOperationCount;
                    }
                }
            }
        }

        YT_VERIFY(std::ssize(operationIds) == std::ssize(failedOperationIds) + std::ssize(removedOperationIds) + lockedOperationCount);
        int removedCount = std::ssize(removedOperationIds);

        RemovedOperationCounter_.Increment(std::ssize(removedOperationIds));
        RemoveOperationErrorCounter_.Increment(std::ssize(failedOperationIds) + lockedOperationCount);

        ProcessRemovedOperations(removedOperationIds);

        for (auto operationId : failedOperationIds) {
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
        VERIFY_INVOKER_AFFINITY(GetCancelableInvoker());

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
        VERIFY_INVOKER_AFFINITY(GetCancelableInvoker());

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
        const TOrderedByIdTableDescriptor descriptor;
        std::vector<TOperationId> ids;
        ids.reserve(requests.size());
        for (const auto& req : requests) {
            ids.push_back(req.Id);
        }
        auto filter = TColumnFilter({descriptor.Index.BriefProgress});
        auto briefProgressIndex = filter.GetPosition(descriptor.Index.BriefProgress);
        auto timeout = Config_->FinishedOperationsArchiveLookupTimeout;
        auto rowsetOrError = LookupOperationsInArchive(Client_, ids, filter, timeout);
        if (!rowsetOrError.IsOK()) {
            YT_LOG_WARNING("Failed to fetch operation brief progress from archive (Error: %v)",
                rowsetOrError);
            return;
        }
        auto rows = rowsetOrError.Value()->GetRows();
        YT_VERIFY(rows.size() == requests.size());
        for (int i = 0; i < static_cast<int>(requests.size()); ++i) {
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

    void DoFetchFinishedOperations()
    {
        YT_LOG_INFO("Fetching all finished operations from Cypress");

        auto listOperationsResult = ListOperations(BIND(&TImpl::CreateBatchRequest, MakeStrong(this)));
        DoFetchFinishedOperationsById(std::move(listOperationsResult.OperationsToArchive));
    }

    void DoFetchFinishedOperationsById(std::vector<TOperationId> operationIds)
    {
        YT_LOG_INFO("Started fetching finished operations from Cypress (OperationCount: %v)", operationIds.size());
        auto operations = FetchOperationsFromCypressForCleaner(
            operationIds,
            BIND(&TImpl::CreateBatchRequest, MakeStrong(this)),
            Config_->ParseOperationAttributesBatchSize,
            Host_->GetBackgroundInvoker());

        // Controller agent reports brief_progress only to archive,
        // but it is necessary to fill ordered_by_start_time table,
        // so we request it here.
        FetchBriefProgressFromArchive(operations);

        // NB: needed for us to store the latest operation for each alias in operation_aliases archive table.
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
        VERIFY_INVOKER_AFFINITY(GetCancelableInvoker());

        return GetOrCrash(OperationMap_, operationId);
    }

    void ProcessRemovedOperations(const std::vector<TOperationId>& removedOperationIds)
    {
        std::vector<TArchiveOperationRequest> removedOperationRequests;
        removedOperationRequests.reserve(removedOperationIds.size());
        for (const auto& operationId : removedOperationIds) {
            auto it = OperationMap_.find(operationId);
            if (it != OperationMap_.end()) {
                removedOperationRequests.emplace_back(std::move(it->second));
                OperationMap_.erase(it);
            }
        }

        OperationsRemovedFromCypress_.Fire(removedOperationRequests);
    }

    void SendOperationAlerts()
    {
        VERIFY_INVOKER_AFFINITY(GetCancelableInvoker());

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
        }
        EnqueuedAlertEvents_.store(std::ssize(OperationAlertEventQueue_));
    }

    void CheckAndTruncateAlertEvents()
    {
        while (std::ssize(OperationAlertEventQueue_) > Config_->MaxEnqueuedOperationAlertEventCount) {
            OperationAlertEventQueue_.pop_front();
        }
        EnqueuedAlertEvents_.store(std::ssize(OperationAlertEventQueue_));
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

void TOperationsCleaner::SubmitForRemoval(std::vector<TOperationId> operationIds)
{
    Impl_->SubmitForRemoval(std::move(operationIds));
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

////////////////////////////////////////////////////////////////////////////////

struct TOperationDataToParse final
{
    TYsonString AttrbutesYson;
    TOperationId OperationId;
};

std::vector<TArchiveOperationRequest> FetchOperationsFromCypressForCleaner(
    const std::vector<TOperationId>& operationIds,
    TCallback<TObjectServiceProxy::TReqExecuteBatchPtr()> createBatchRequest,
    const int parseOperationAttributesBatchSize,
    const IInvokerPtr& invoker)
{
    using NYT::ToProto;

    YT_LOG_INFO("Fetching operations attributes for cleaner (OperationCount: %v)", operationIds.size());

    std::vector<TArchiveOperationRequest> result;

    auto batchReq = createBatchRequest();

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

    {
        const auto processBatch = BIND([parseOperationAttributesBatchSize] (
            const std::vector<TOperationDataToParse>& operationDataToParseBatch)
        {
            std::vector<TArchiveOperationRequest> result;
            result.reserve(parseOperationAttributesBatchSize);

            for (const auto& operationDataToParse : operationDataToParseBatch) {
                IAttributeDictionaryPtr attributes;
                TOperationId operationId;
                try {
                    attributes = ConvertToAttributes(operationDataToParse.AttrbutesYson);
                    operationId = TOperationId::FromString(attributes->Get<TString>("key"));
                    YT_VERIFY(operationId == operationDataToParse.OperationId);
                } catch (const std::exception& ex) {
                    THROW_ERROR_EXCEPTION("Error parsing operation attributes")
                        << TErrorAttribute("operation_id", operationDataToParse.OperationId)
                        << ex;
                }

                try {
                    TArchiveOperationRequest req;
                    req.InitializeFromAttributes(*attributes);
                    result.push_back(std::move(req));
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

        const int operationCount{static_cast<int>(operationIds.size())};
        std::vector<TFuture<std::vector<TArchiveOperationRequest>>> futures;
        futures.reserve(RoundUp(operationCount, parseOperationAttributesBatchSize));

        for (int startIndex = 0; startIndex < operationCount; startIndex += parseOperationAttributesBatchSize) {
            std::vector<TOperationDataToParse> operationDataToParseBatch;
            operationDataToParseBatch.reserve(parseOperationAttributesBatchSize);

            for (int index = startIndex; index < std::min(operationCount, startIndex + parseOperationAttributesBatchSize); ++index) {
                operationDataToParseBatch.push_back({TYsonString(rsps[index].Value()->value()), operationIds[index]});
            }

            futures.push_back(processBatch
                .AsyncVia(invoker)
                .Run(std::move(operationDataToParseBatch))
            );
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

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
