#include "operation.h"

#include "operation_controller.h"
#include "exec_node.h"
#include "helpers.h"
#include "allocation.h"
#include "controller_agent.h"

#include <yt/yt/server/lib/scheduler/experiments.h>

#include <yt/yt/ytlib/scheduler/helpers.h>
#include <yt/yt/ytlib/scheduler/config.h>

#include <yt/yt/ytlib/api/native/connection.h>
#include <yt/yt/ytlib/api/native/client.h>

#include <yt/yt/client/api/transaction.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/core/actions/cancelable_context.h>

namespace NYT::NScheduler {

using namespace NApi;
using namespace NCypressClient;
using namespace NTransactionClient;
using namespace NObjectClient;
using namespace NSecurityClient;
using namespace NRpc;
using namespace NYTree;
using namespace NYson;

using NYT::FromProto;
using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

constexpr int MaxAnnotationValueLength = 128;

static constexpr auto& Logger = SchedulerLogger;

////////////////////////////////////////////////////////////////////////////////

void TOperationPoolTreeAttributes::Register(TRegistrar registrar)
{
    registrar.Parameter("slot_index", &TThis::SlotIndex)
        .Default();

    registrar.Parameter("running_in_ephemeral_pool", &TThis::RunningInEphemeralPool)
        .Default();

    registrar.Parameter("running_in_lightweight_pool", &TThis::RunningInLightweightPool)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

void Serialize(const TOperationEvent& event, IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer)
        .BeginMap()
            .Item("time").Value(event.Time)
            .Item("state").Value(event.State)
            .OptionalItem("attributes", event.Attributes)
        .EndMap();
}

void Deserialize(TOperationEvent& event, INodePtr node)
{
    auto mapNode = node->AsMap();
    event.Time = ConvertTo<TInstant>(mapNode->GetChildOrThrow("time"));
    event.State = ConvertTo<EOperationState>(mapNode->GetChildOrThrow("state"));
    if (auto attributes = mapNode->FindChild("attributes")) {
        event.Attributes = ConvertToYsonString(attributes);
    }
}

////////////////////////////////////////////////////////////////////////////////

void Deserialize(TOperationAlert& alert, NYTree::INodePtr node)
{
    Deserialize(alert.Error, node);
}

////////////////////////////////////////////////////////////////////////////////

TOperation::TOperation(
    TOperationId id,
    EOperationType type,
    TMutationId mutationId,
    TTransactionId userTransactionId,
    TOperationSpecBasePtr spec,
    THashMap<TString, TStrategyOperationSpecPtr> customSpecPerTree,
    TYsonString specString,
    TYsonString trimmedAnnotations,
    std::optional<TBriefVanillaTaskSpecMap> briefVanillaTaskSpecs,
    IMapNodePtr secureVault,
    std::optional<TNodeId> temporaryTokenNodeId,
    TOperationRuntimeParametersPtr runtimeParameters,
    NSecurityClient::TSerializableAccessControlList baseAcl,
    const std::string& authenticatedUser,
    TInstant startTime,
    IInvokerPtr controlInvoker,
    const std::optional<TString>& alias,
    std::vector<TExperimentAssignmentPtr> experimentAssignments,
    NYson::TYsonString providedSpecString,
    EOperationState state,
    const std::vector<TOperationEvent>& events,
    bool suspended,
    int registrationIndex,
    const THashMap<EOperationAlertType, TOperationAlert>& alerts)
    : MutationId_(mutationId)
    , Suspended_(suspended)
    , UserTransactionId_(userTransactionId)
    , SecureVault_(std::move(secureVault))
    , TemporaryTokenNodeId_(temporaryTokenNodeId)
    , Events_(events)
    , Spec_(std::move(spec))
    , ProvidedSpecString_(std::move(providedSpecString))
    , SuspiciousJobs_(NYson::TYsonString(TString(), NYson::EYsonType::MapFragment))
    , Alias_(alias)
    , BaseAcl_(std::move(baseAcl))
    , ExperimentAssignments_(std::move(experimentAssignments))
    , RegistrationIndex_(registrationIndex)
    , Id_(id)
    , Type_(type)
    , StartTime_(startTime)
    , AuthenticatedUser_(authenticatedUser)
    , SpecString_(specString)
    , TrimmedAnnotations_(std::move(trimmedAnnotations))
    , BriefVanillaTaskSpecs_(std::move(briefVanillaTaskSpecs))
    , CustomSpecPerTree_(std::move(customSpecPerTree))
    , Codicil_(MakeOperationCodicil(Id_))
    , ControlInvoker_(std::move(controlInvoker))
    , State_(state)
    , RuntimeParameters_(std::move(runtimeParameters))
    , Alerts_(alerts.begin(), alerts.end())
{
    YT_VERIFY(SpecString_);
    Restart(TError()); // error is fake
}

EOperationType TOperation::GetType() const
{
    return Type_;
}

TOperationId TOperation::GetId() const
{
    return Id_;
}

TInstant TOperation::GetStartTime() const
{
    return StartTime_;
}

TString TOperation::GetAuthenticatedUser() const
{
    return AuthenticatedUser_;
}

TStrategyOperationSpecPtr TOperation::GetStrategySpec() const
{
    return Spec_;
}

TStrategyOperationSpecPtr TOperation::GetStrategySpecForTree(const TString& treeId) const
{
    auto it = CustomSpecPerTree_.find(treeId);
    if (it != CustomSpecPerTree_.end()) {
        return it->second;
    } else {
        return Spec_;
    }
}

const TYsonString& TOperation::GetSpecString() const
{
    return SpecString_;
}

const TYsonString& TOperation::GetTrimmedAnnotations() const
{
    return TrimmedAnnotations_;
}

std::vector<std::string> TOperation::GetTaskNames() const
{
    return BriefVanillaTaskSpecs_
        ? GetKeys(*BriefVanillaTaskSpecs_)
        : std::vector<std::string>{};
}

const std::optional<TBriefVanillaTaskSpecMap>& TOperation::GetMaybeBriefVanillaTaskSpecs() const
{
    return BriefVanillaTaskSpecs_;
}

TFuture<TOperationPtr> TOperation::GetStarted()
{
    return StartedPromise_.ToFuture().Apply(BIND([this_ = MakeStrong(this)] () -> TOperationPtr {
        return this_;
    }));
}

TAccessControlRule TOperation::GetAccessControlRule() const
{
    if (RuntimeParameters_->AcoName) {
        return *RuntimeParameters_->AcoName;
    } else {
        return RuntimeParameters_->Acl;
    }
}

void TOperation::SetStarted(const TError& error)
{
    StartedPromise_.Set(error);
}

TFuture<void> TOperation::GetFinished()
{
    return FinishedPromise_;
}

void TOperation::SetFinished()
{
    FinishedPromise_.Set();
    Suspended_ = false;
    for (auto& [_, alert] : Alerts_) {
        NConcurrency::TDelayedExecutor::CancelAndClear(alert.ResetCookie);
    }
    Alerts_.clear();
}

bool TOperation::GetUnregistering() const
{
    return Unregistering_;
}

void TOperation::SetUnregistering()
{
    Unregistering_ = true;
}

bool TOperation::IsFinishedState() const
{
    return IsOperationFinished(State_);
}

bool TOperation::IsFinishingState() const
{
    return IsOperationFinishing(State_);
}

std::optional<EUnschedulableReason> TOperation::CheckUnschedulable(const std::optional<TString>& treeId) const
{
    if (State_ != EOperationState::Running) {
        return EUnschedulableReason::IsNotRunning;
    }

    if (Suspended_) {
        return EUnschedulableReason::Suspended;
    }

    if (treeId) {
        if (Controller_->GetNeededResources().GetNeededResourcesForTree(treeId.value()).GetUserSlots() == 0) {
            return EUnschedulableReason::NoPendingAllocations;
        }
    } else if (Controller_->GetNeededResources().DefaultResources.GetUserSlots() == 0) {
        // Check needed resources of all trees.
        bool noPendingAllocations = true;
        for (const auto& [treeId, neededResources] : Controller_->GetNeededResources().ResourcesByPoolTree) {
            noPendingAllocations = noPendingAllocations && neededResources.GetUserSlots() == 0;
        }
        if (noPendingAllocations) {
            return EUnschedulableReason::NoPendingAllocations;
        }
    }

    return std::nullopt;
}

IOperationControllerStrategyHostPtr TOperation::GetControllerStrategyHost() const
{
    return Controller_;
}

TCodicilGuard TOperation::MakeCodicilGuard() const
{
    return TCodicilGuard(MakeOwningCodicilBuilder(Codicil_));
}

EOperationState TOperation::GetState() const
{
    return State_;
}

void TOperation::SetStateAndEnqueueEvent(
    EOperationState state,
    TYsonString attributes)
{
    static TYsonString DefaultAttributes = BuildYsonStringFluently()
        .BeginMap()
        .EndMap();
    if (!attributes) {
        attributes = DefaultAttributes;
    }
    State_ = state;
    Events_.emplace_back(TOperationEvent({TInstant::Now(), state, std::move(attributes)}));
    ShouldFlush_ = true;
}

void TOperation::SetSlotIndex(const TString& treeId, int value)
{
    ShouldFlush_ = true;
    SchedulingAttributesPerPoolTree_[treeId].SlotIndex = value;
}

void TOperation::ReleaseSlotIndex(const TString& treeId)
{
    auto& slotIndex = SchedulingAttributesPerPoolTree_[treeId].SlotIndex;

    YT_VERIFY(slotIndex);

    slotIndex.reset();
}

std::optional<int> TOperation::FindSlotIndex(const TString& treeId) const
{
    auto it = SchedulingAttributesPerPoolTree_.find(treeId);
    return it != SchedulingAttributesPerPoolTree_.end() ? it->second.SlotIndex : std::nullopt;
}

const THashMap<TString, TOperationPoolTreeAttributes>& TOperation::GetSchedulingAttributesPerPoolTree() const
{
    return SchedulingAttributesPerPoolTree_;
}

THashMap<TString, int> TOperation::GetSlotIndices() const
{
    THashMap<TString, int> treeIdToSlotIndex;
    treeIdToSlotIndex.reserve(SchedulingAttributesPerPoolTree_.size());
    for (const auto& [treeId, schedulingInfo] : SchedulingAttributesPerPoolTree_) {
        if (schedulingInfo.SlotIndex) {
            EmplaceOrCrash(treeIdToSlotIndex, treeId, *schedulingInfo.SlotIndex);
        }
    }
    return treeIdToSlotIndex;
}

TOperationRuntimeParametersPtr TOperation::GetRuntimeParameters() const
{
    return RuntimeParameters_;
}

void TOperation::UpdatePoolAttributes(
    const TString& treeId,
    const TOperationPoolTreeAttributes& operationPoolTreeAttributes)
{
    auto& schedulingAttributes = GetOrCrash(SchedulingAttributesPerPoolTree_, treeId);

    if (schedulingAttributes.RunningInEphemeralPool != operationPoolTreeAttributes.RunningInEphemeralPool ||
        schedulingAttributes.RunningInLightweightPool != operationPoolTreeAttributes.RunningInLightweightPool)
    {
        SetShouldFlush(true);
    }
    schedulingAttributes.RunningInEphemeralPool = operationPoolTreeAttributes.RunningInEphemeralPool;
    schedulingAttributes.RunningInLightweightPool = operationPoolTreeAttributes.RunningInLightweightPool;
}

bool TOperation::IsRunningInStrategy() const
{
    return RunningInStrategy_;
}

void TOperation::SetRunningInStrategy()
{
    RunningInStrategy_= true;
}

void TOperation::SetRuntimeParameters(TOperationRuntimeParametersPtr parameters)
{
    bool hasAcl = !RuntimeParameters_->Acl.Entries.empty() || !parameters->Acl.Entries.empty();
    bool hasAcoName = RuntimeParameters_->AcoName || parameters->AcoName;

    YT_VERIFY(!hasAcl || !hasAcoName);

    SetShouldFlush(true);
    RuntimeParameters_ = std::move(parameters);
}

TYsonString TOperation::BuildAlertsString() const
{
    auto result = BuildYsonStringFluently()
        .DoMapFor(Alerts_, [&] (TFluentMap fluent, const auto& pair) {
            const auto& [alertType, alert] = pair;

            fluent
                .Item(FormatEnum(alertType)).Value(alert.Error);
        });

    return result;
}

bool TOperation::HasAlert(EOperationAlertType alertType) const
{
    return Alerts_.find(alertType) != Alerts_.end();
}

bool TOperation::HasAlertResetCookie(EOperationAlertType alertType) const
{
    auto it = Alerts_.find(alertType);
    YT_VERIFY(it != Alerts_.end());
    return static_cast<bool>(it->second.ResetCookie);
}

bool TOperation::SetAlertWithoutArchivation(EOperationAlertType alertType, const TError& error)
{
    auto& alert = Alerts_[alertType];

    if (alert.Error.GetSkeleton() == error.GetSkeleton()) {
        return false;
    }

    alert.Error = error;
    ShouldFlush_ = true;

    return true;
}

void TOperation::ResetAlertWithoutArchivation(EOperationAlertType alertType)
{
    auto it = Alerts_.find(alertType);
    if (it == Alerts_.end()) {
        return;
    }
    NConcurrency::TDelayedExecutor::CancelAndClear(it->second.ResetCookie);
    Alerts_.erase(it);
    ShouldFlush_ = true;
}

void TOperation::SetAlertResetCookie(EOperationAlertType alertType, NConcurrency::TDelayedExecutorCookie cookie)
{
    auto it = Alerts_.find(alertType);
    YT_VERIFY(it != Alerts_.end());
    YT_VERIFY(!it->second.ResetCookie);
    it->second.ResetCookie = cookie;
}

const IInvokerPtr& TOperation::GetCancelableControlInvoker()
{
    return CancelableInvoker_;
}

const IInvokerPtr& TOperation::GetControlInvoker()
{
    return ControlInvoker_;
}

void TOperation::Cancel(const TError& error)
{
    if (CancelableContext_) {
        CancelableContext_->Cancel(error);
    }
}

void TOperation::Restart(const TError& error)
{
    Cancel(error);
    CancelableContext_ = New<TCancelableContext>();
    CancelableInvoker_ = CancelableContext_->CreateInvoker(ControlInvoker_);
}

TYsonString TOperation::BuildResultString() const
{
    auto error = NYT::FromProto<TError>(Result_.error());
    return BuildYsonStringFluently()
        .BeginMap()
            .Item("error").Value(error)
        .EndMap();
}

void TOperation::SetAgent(const TControllerAgentPtr& agent)
{
    Agent_ = agent;
}

TControllerAgentPtr TOperation::FindAgent()
{
    return Agent_.Lock();
}

TControllerAgentPtr TOperation::GetAgentOrThrow()
{
    auto agent = FindAgent();
    if (!agent) {
        THROW_ERROR_EXCEPTION("Operation %v is not assigned to any agent",
            Id_);
    }
    return agent;
}

bool TOperation::IsTreeErased(const TString& treeId) const
{
    const auto& erasedTrees = RuntimeParameters_->ErasedTrees;
    return std::find(erasedTrees.begin(), erasedTrees.end(), treeId) != erasedTrees.end();
}

bool TOperation::AreAllTreesErased() const
{
    return RuntimeParameters_->SchedulingOptionsPerPoolTree.empty();
}

void TOperation::EraseTrees(const std::vector<TString>& treeIds)
{
    if (!treeIds.empty()) {
        ShouldFlush_ = true;
    }

    for (const auto& treeId : treeIds) {
        RuntimeParameters_->ErasedTrees.push_back(treeId);
        EraseOrCrash(RuntimeParameters_->SchedulingOptionsPerPoolTree, treeId);
    }
}

std::vector<TString> TOperation::GetExperimentAssignmentNames() const
{
    std::vector<TString> result;
    result.reserve(ExperimentAssignments_.size());
    for (const auto& experimentAssignment : ExperimentAssignments_) {
        result.emplace_back(experimentAssignment->GetName());
    }
    return result;
}

std::vector<TString> TOperation::GetJobShellOwners(const TString& jobShellName)
{
    TJobShellPtr jobShell;
    for (const auto& shell : Spec_->JobShells) {
        if (shell->Name == jobShellName) {
            jobShell = shell;
        }
    }

    if (!jobShell) {
        THROW_ERROR_EXCEPTION(
            NScheduler::EErrorCode::NoSuchJobShell,
            "Job shell %Qv not found",
            jobShellName);
    }

    const auto& optionsPerJobShell = RuntimeParameters_->OptionsPerJobShell;
    if (auto it = optionsPerJobShell.find(jobShellName); it != optionsPerJobShell.end()) {
        const auto& options = it->second;
        return options->Owners;
    }
    return jobShell->Owners;
}

TFuture<void> TOperation::AbortCommonTransactions()
{
    YT_VERIFY(Transactions_);
    const auto Logger = SchedulerLogger().WithTag("OperationId", GetId());
    std::vector<TFuture<void>> asyncResults;
    THashSet<ITransactionPtr> abortedTransactions;
    auto scheduleAbort = [&] (const ITransactionPtr& transaction, TString transactionType) {
        if (abortedTransactions.contains(transaction)) {
            return;
        }

        if (transaction) {
            YT_LOG_DEBUG("Aborting transaction (TransactionId: %v, Type: %v)",
                transaction->GetId(),
                transactionType);
            YT_VERIFY(abortedTransactions.emplace(transaction).second);
            asyncResults.push_back(transaction->Abort());
        } else {
            YT_LOG_DEBUG("Transaction is missing, skipping abort (Type: %v)",
                transactionType);
        }
    };

    scheduleAbort(Transactions_->AsyncTransaction, "Async");
    scheduleAbort(Transactions_->InputTransaction, "Input");
    scheduleAbort(Transactions_->OutputTransaction, "Output");
    scheduleAbort(Transactions_->DebugTransaction, "Debug");
    for (const auto& transaction : Transactions_->InputTransactions) {
        scheduleAbort(transaction, "Input");
    }
    for (const auto& transaction : Transactions_->NestedInputTransactions) {
        scheduleAbort(transaction, "NestedInput");
    }

    return AllSucceeded(
        asyncResults,
        {
            .PropagateCancelationToInput = false,
            .CancelInputOnShortcut = false,
        });
}

bool TOperation::AddSecureVaultEntry(const TString& key, const INodePtr& value)
{
    YT_VERIFY(State_ == EOperationState::Starting);

    if (!SecureVault_) {
        YT_LOG_DEBUG("Creating empty secure vault due to scheduler request (OperationId: %v)",
            Id_);
        SecureVault_ = GetEphemeralNodeFactory()->CreateMap();
    }

    YT_LOG_DEBUG("Adding secure vault entry (OperationId: %v, Key: %v)",
        Id_,
        key);
    return SecureVault_->AddChild(key, value);
}

void TOperation::SetTemporaryToken(const TString& token, const TNodeId& nodeId)
{
    YT_VERIFY(State_ == EOperationState::Starting);
    YT_VERIFY(Spec_->IssueTemporaryToken);
    // We check that the key is not already present in the secure vault before calling this method.
    YT_VERIFY(AddSecureVaultEntry(Spec_->TemporaryTokenEnvironmentVariableName, ConvertToNode(token)));

    TemporaryTokenNodeId_ = nodeId;
}

std::vector<TNodeId> TOperation::GetDependentNodeIds() const
{
    if (TemporaryTokenNodeId_) {
        return {*TemporaryTokenNodeId_};
    }

    return {};
}

////////////////////////////////////////////////////////////////////////////////

TYsonString TrimAnnotations(const IMapNodePtr& annotationsNode)
{
    return BuildYsonStringFluently()
        .DoMapFor(annotationsNode->GetChildren(), [&] (TFluentMap fluent, const auto& pair) {
            const auto& [key, child] = pair;
            switch (child->GetType())  {
                case ENodeType::String: {
                    auto trimmedString = child->AsString()->GetValue();
                    if (std::ssize(trimmedString) > MaxAnnotationValueLength) {
                        trimmedString = Format("%v...<message truncated>", trimmedString.substr(0, MaxAnnotationValueLength));
                    }
                    fluent.Item(key).Value(trimmedString);
                    break;
                }
                case ENodeType::Int64:
                case ENodeType::Uint64:
                case ENodeType::Double:
                case ENodeType::Boolean:
                    fluent.Item(key).Value(child);
                    break;
                default:  // Ignore nested annotations.
                    break;
            }
        });
}

void ParseSpec(
    IMapNodePtr specNode,
    INodePtr specTemplate,
    EOperationType operationType,
    std::optional<TOperationId> operationId,
    TPreprocessedSpec* preprocessedSpec)
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    if (specTemplate) {
        specNode = PatchNode(specTemplate, specNode)->AsMap();
    }

    if (operationId) { // Revive case
        try {
            if (auto aclNode = specNode->FindChild("acl")) {
                ConvertTo<TSerializableAccessControlList>(aclNode);
            }
        } catch (const std::exception& ex) {
            YT_LOG_WARNING(ex, "Failed to parse operation ACL from spec, removing it (OperationId: %v)",
                *operationId);
            specNode->RemoveChild("acl");
        }
    }

    try {
        preprocessedSpec->Spec = ConvertTo<TOperationSpecBasePtr>(specNode);
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Error parsing operation spec")
            << ex;
    }

    if (operationType == EOperationType::Vanilla) {
        TVanillaOperationSpecPtr vanillaOperationSpec;
        try {
            vanillaOperationSpec = ConvertTo<TVanillaOperationSpecPtr>(specNode);
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Error parsing vanilla operation spec")
                << ex;
        }

        preprocessedSpec->BriefVanillaTaskSpecs.emplace();
        preprocessedSpec->BriefVanillaTaskSpecs->reserve(size(vanillaOperationSpec->Tasks));
        for (const auto& [taskName, taskSpec] : vanillaOperationSpec->Tasks) {
            preprocessedSpec->BriefVanillaTaskSpecs->emplace(
                taskName,
                TBriefVanillaTaskSpec{
                    .JobCount = taskSpec->JobCount,
                });
        }
    }

    specNode->RemoveChild("secure_vault");
    preprocessedSpec->SpecString = ConvertToYsonString(specNode);

    if (auto annotationsNode = specNode->FindChild("annotations")) {
        preprocessedSpec->TrimmedAnnotations = TrimAnnotations(annotationsNode->AsMap());
    }

    auto strategySpec = static_cast<TStrategyOperationSpecPtr>(preprocessedSpec->Spec);
    for (const auto& [treeId, optionPerPoolTree] : strategySpec->SchedulingOptionsPerPoolTree) {
        preprocessedSpec->CustomSpecPerTree.emplace(
            treeId,
            UpdateYsonStruct(strategySpec, ConvertToNode(optionPerPoolTree)));
    }
}

IMapNodePtr ConvertSpecStringToNode(
    const TYsonString& specString,
    int treeSizeLimit)
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    IMapNodePtr specNode;
    try {
        specNode = ConvertToNode(specString, treeSizeLimit)->AsMap();
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Error parsing operation spec string")
            << ex;
    }

    return specNode;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
