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

static const auto& Logger = SchedulerLogger;

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

void ToProto(
    NControllerAgent::NProto::TControllerTransactionIds* transactionIdsProto,
    const TOperationTransactions& transactions)
{
    auto getId = [] (const NApi::ITransactionPtr& transaction) {
        return transaction ? transaction->GetId() : NTransactionClient::TTransactionId();
    };

    ToProto(transactionIdsProto->mutable_async_id(), getId(transactions.AsyncTransaction));
    ToProto(transactionIdsProto->mutable_input_id(), getId(transactions.InputTransaction));
    ToProto(transactionIdsProto->mutable_output_id(), getId(transactions.OutputTransaction));
    ToProto(transactionIdsProto->mutable_debug_id(), getId(transactions.DebugTransaction));
    ToProto(transactionIdsProto->mutable_output_completion_id(), getId(transactions.OutputCompletionTransaction));
    ToProto(transactionIdsProto->mutable_debug_completion_id(), getId(transactions.DebugCompletionTransaction));

    for (const auto& transaction : transactions.NestedInputTransactions) {
        ToProto(transactionIdsProto->add_nested_input_ids(), getId(transaction));
    }
}

void FromProto(
    TOperationTransactions* transactions,
    const NControllerAgent::NProto::TControllerTransactionIds& transactionIdsProto,
    std::function<NNative::IClientPtr(TCellTag)> getClient,
    TDuration pingPeriod)
{
    THashMap<TTransactionId, ITransactionPtr> transactionIdToTransaction;
    auto attachTransaction = [&] (TTransactionId transactionId) -> ITransactionPtr {
        if (!transactionId) {
            return nullptr;
        }

        auto it = transactionIdToTransaction.find(transactionId);
        if (it == transactionIdToTransaction.end()) {
            auto client = getClient(CellTagFromId(transactionId));

            TTransactionAttachOptions options;
            options.Ping = true;
            options.PingAncestors = false;
            options.PingPeriod = pingPeriod;

            auto transaction = client->AttachTransaction(transactionId, options);
            YT_VERIFY(transactionIdToTransaction.emplace(transactionId, transaction).second);
            return transaction;
        } else {
            return it->second;
        }
    };

    transactions->AsyncTransaction = attachTransaction(FromProto<TTransactionId>(transactionIdsProto.async_id()));
    transactions->InputTransaction = attachTransaction(FromProto<TTransactionId>(transactionIdsProto.input_id()));
    transactions->OutputTransaction = attachTransaction(FromProto<TTransactionId>(transactionIdsProto.output_id()));
    transactions->DebugTransaction = attachTransaction(FromProto<TTransactionId>(transactionIdsProto.debug_id()));
    transactions->OutputCompletionTransaction = attachTransaction(FromProto<TTransactionId>(transactionIdsProto.output_completion_id()));
    transactions->DebugCompletionTransaction = attachTransaction(FromProto<TTransactionId>(transactionIdsProto.debug_completion_id()));

    auto nestedInputTransactionIds = FromProto<std::vector<TTransactionId>>(transactionIdsProto.nested_input_ids());
    for (auto transactionId : nestedInputTransactionIds) {
        transactions->NestedInputTransactions.push_back(attachTransaction(transactionId));
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
    std::vector<TString> vanillaTaskNames,
    IMapNodePtr secureVault,
    TOperationRuntimeParametersPtr runtimeParameters,
    NSecurityClient::TSerializableAccessControlList baseAcl,
    const TString& authenticatedUser,
    TInstant startTime,
    IInvokerPtr controlInvoker,
    const std::optional<TString>& alias,
    std::vector<TExperimentAssignmentPtr> experimentAssignments,
    NYson::TYsonString providedSpecString,
    EOperationState state,
    const std::vector<TOperationEvent>& events,
    bool suspended,
    const std::optional<TJobResources>& aggregatedInitialMinNeededResources,
    int registrationIndex,
    const THashMap<EOperationAlertType, TOperationAlert>& alerts)
    : MutationId_(mutationId)
    , Suspended_(suspended)
    , UserTransactionId_(userTransactionId)
    , SecureVault_(std::move(secureVault))
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
    , VanillaTaskNames_(std::move(vanillaTaskNames))
    , CustomSpecPerTree_(std::move(customSpecPerTree))
    , CodicilData_(MakeOperationCodicilString(Id_))
    , ControlInvoker_(std::move(controlInvoker))
    , State_(state)
    , RuntimeParameters_(std::move(runtimeParameters))
    , Alerts_(alerts.begin(), alerts.end())
    , AggregatedInitialMinNeededResources_(aggregatedInitialMinNeededResources)
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

const std::vector<TString>& TOperation::GetTaskNames() const
{
    return VanillaTaskNames_;
}

TFuture<TOperationPtr> TOperation::GetStarted()
{
    return StartedPromise_.ToFuture().Apply(BIND([this_ = MakeStrong(this)] () -> TOperationPtr {
        return this_;
    }));
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
    return TCodicilGuard(CodicilData_);
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
    TreeIdToSlotIndex_[treeId] = value;
}

void TOperation::ReleaseSlotIndex(const TString& treeId)
{
    EraseOrCrash(TreeIdToSlotIndex_, treeId);
}

std::optional<int> TOperation::FindSlotIndex(const TString& treeId) const
{
    auto it = TreeIdToSlotIndex_.find(treeId);
    return it != TreeIdToSlotIndex_.end() ? std::make_optional(it->second) : std::nullopt;
}

const THashMap<TString, int>& TOperation::GetSlotIndices() const
{
    return TreeIdToSlotIndex_;
}

TOperationRuntimeParametersPtr TOperation::GetRuntimeParameters() const
{
    return RuntimeParameters_;
}

bool TOperation::IsRunningInStrategy() const
{
    return RunningInStrategy_;
}

void TOperation::SetRunningInStrategy()
{
    RunningInStrategy_= true;
}

std::optional<TJobResources> TOperation::GetAggregatedInitialMinNeededResources() const
{
    return AggregatedInitialMinNeededResources_;
}

void TOperation::SetRuntimeParameters(TOperationRuntimeParametersPtr parameters)
{
    if (parameters->Acl != RuntimeParameters_->Acl) {
        SetShouldFlushAcl(true);
    }
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
    VERIFY_THREAD_AFFINITY_ANY();

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

        preprocessedSpec->VanillaTaskNames.reserve(vanillaOperationSpec->Tasks.size());
        for (const auto& [taskName, _] : vanillaOperationSpec->Tasks) {
            preprocessedSpec->VanillaTaskNames.push_back(taskName);
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

IMapNodePtr ConvertSpecStringToNode(const TYsonString& specString)
{
    VERIFY_THREAD_AFFINITY_ANY();

    IMapNodePtr specNode;
    try {
        specNode = ConvertToNode(specString)->AsMap();
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Error parsing operation spec string")
            << ex;
    }

    return specNode;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler

