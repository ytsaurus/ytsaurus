#include "operation.h"
#include "operation_controller.h"
#include "exec_node.h"
#include "helpers.h"
#include "job.h"
#include "controller_agent.h"

#include <yt/ytlib/scheduler/helpers.h>
#include <yt/ytlib/scheduler/config.h>

#include <yt/core/actions/cancelable_context.h>

namespace NYT {
namespace NScheduler {

using namespace NApi;
using namespace NTransactionClient;
using namespace NJobTrackerClient;
using namespace NRpc;
using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

void Serialize(const TOperationEvent& event, IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer)
        .BeginMap()
            .Item("time").Value(event.Time)
            .Item("state").Value(event.State)
        .EndMap();
}

void Deserialize(TOperationEvent& event, INodePtr node)
{
    auto mapNode = node->AsMap();
    event.Time = ConvertTo<TInstant>(mapNode->GetChild("time"));
    event.State = ConvertTo<EOperationState>(mapNode->GetChild("state"));
}

////////////////////////////////////////////////////////////////////////////////

TOperation::TOperation(
    const TOperationId& id,
    EOperationType type,
    const TMutationId& mutationId,
    const TTransactionId& userTransactionId,
    IMapNodePtr spec,
    IMapNodePtr secureVault,
    TOperationRuntimeParametersPtr runtimeParams,
    const TString& authenticatedUser,
    TInstant startTime,
    bool enableCompatibleStorageMode,
    IInvokerPtr controlInvoker,
    EOperationState state,
    const std::vector<TOperationEvent>& events)
    : Type_(type)
    , MutationId_(mutationId)
    , State_(state)
    , UserTransactionId_(userTransactionId)
    , RuntimeParameters_(std::move(runtimeParams))
    , RuntimeData_(New<TOperationRuntimeData>())
    , SecureVault_(std::move(secureVault))
    , Events_(events)
    , EnableCompatibleStorageMode_(enableCompatibleStorageMode)
    , SuspiciousJobs_(NYson::TYsonString(TString(), NYson::EYsonType::MapFragment))
    , Id_(id)
    , StartTime_(startTime)
    , AuthenticatedUser_(authenticatedUser)
    , Spec_(spec)
    , CodicilData_(MakeOperationCodicilString(Id_))
    , ControlInvoker_(std::move(controlInvoker))
{
    YCHECK(Spec_);
    Restart();

    if (RuntimeParameters_->Owners) {
        Owners_ = *RuntimeParameters_->Owners;
    }
}

const TOperationId& TOperation::GetId() const
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

NYTree::IMapNodePtr TOperation::GetSpec() const
{
    return Spec_;
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
    for (auto& pair : Alerts_) {
        NConcurrency::TDelayedExecutor::CancelAndClear(pair.second.ResetCookie);
    }
    Alerts_.clear();
}

bool TOperation::IsFinishedState() const
{
    return IsOperationFinished(State_);
}

bool TOperation::IsFinishingState() const
{
    return IsOperationFinishing(State_);
}

bool TOperation::IsSchedulable() const
{
    return State_ == EOperationState::Running && !Suspended_;
}

IOperationControllerStrategyHostPtr TOperation::GetControllerStrategyHost() const
{
    return Controller_;
}

void TOperation::UpdateControllerTimeStatistics(const NYPath::TYPath& name, TDuration value)
{
    ControllerTimeStatistics_.AddSample(name, value.MicroSeconds());
}

TCodicilGuard TOperation::MakeCodicilGuard() const
{
    return TCodicilGuard(CodicilData_);
}

void TOperation::SetStateAndEnqueueEvent(EOperationState state)
{
    State_ = state;
    Events_.emplace_back(TOperationEvent({TInstant::Now(), state}));
    ShouldFlush_ = true;
}

void TOperation::SetSlotIndex(const TString& treeId, int value)
{
    TreeIdToSlotIndex_.emplace(treeId, value);
}

TNullable<int> TOperation::FindSlotIndex(const TString& treeId) const
{
    auto it = TreeIdToSlotIndex_.find(treeId);
    return it != TreeIdToSlotIndex_.end() ? MakeNullable(it->second) : Null;
}

int TOperation::GetSlotIndex(const TString& treeId) const
{
    auto slotIndex = FindSlotIndex(treeId);
    YCHECK(slotIndex);
    return *slotIndex;
}

const THashMap<TString, int>& TOperation::GetSlotIndices() const
{
    return TreeIdToSlotIndex_;
}

const std::vector<TString>& TOperation::GetOwners() const
{
    return Owners_;
}

void TOperation::SetOwners(std::vector<TString> owners)
{
    Owners_ = std::move(owners);
    ShouldFlush_ = true;
    ShouldFlushAcl_ = true;
}

TYsonString TOperation::BuildAlertsString() const
{
    auto result = BuildYsonStringFluently()
        .DoMapFor(Alerts_, [&] (TFluentMap fluent, const auto& pair) {
            const auto& alertType = pair.first;
            const auto& alert = pair.second;

            fluent
                .Item(FormatEnum(alertType)).Value(alert.Error);
        });

    return result;
}

void TOperation::SetAlert(EOperationAlertType alertType, const TError& error, TNullable<TDuration> timeout)
{
    auto& alert = Alerts_[alertType];

    if (alert.Error.Sanitize() == error.Sanitize()) {
        return;
    }

    alert.Error = error;
    NConcurrency::TDelayedExecutor::CancelAndClear(alert.ResetCookie);

    if (timeout) {
        auto resetCallback = BIND(&TOperation::ResetAlert, MakeStrong(this), alertType)
            .Via(CancelableInvoker_);

        alert.ResetCookie = NConcurrency::TDelayedExecutor::Submit(resetCallback, *timeout);
    }

    ShouldFlush_ = true;
}

void TOperation::ResetAlert(EOperationAlertType alertType)
{
    auto it = Alerts_.find(alertType);
    if (it == Alerts_.end()) {
        return;
    }
    NConcurrency::TDelayedExecutor::CancelAndClear(it->second.ResetCookie);
    Alerts_.erase(it);
    ShouldFlush_ = true;
}

const IInvokerPtr& TOperation::GetCancelableControlInvoker()
{
    return CancelableInvoker_;
}

void TOperation::Cancel()
{
    if (CancelableContext_) {
        CancelableContext_->Cancel();
    }
}

void TOperation::Restart()
{
    Cancel();
    CancelableContext_ = New<TCancelableContext>();
    CancelableInvoker_ = CancelableContext_->CreateInvoker(ControlInvoker_);
}

void TOperation::SetAgent(const TControllerAgentPtr& agent)
{
    Agent_ = agent;
}

TControllerAgentPtr TOperation::GetAgentOrCancelFiber()
{
    auto agent = Agent_.Lock();
    if (!agent) {
        throw NConcurrency::TFiberCanceledException();
    }
    return agent;
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

////////////////////////////////////////////////////////////////////////////////

int TOperationRuntimeData::GetPendingJobCount() const
{
    return PendingJobCount_.load();
}

void TOperationRuntimeData::SetPendingJobCount(int value)
{
    PendingJobCount_.store(value);
}

NScheduler::TJobResources TOperationRuntimeData::GetNeededResources()
{
    NConcurrency::TReaderGuard guard(NeededResourcesLock_);
    return NeededResources_;
}

void TOperationRuntimeData::SetNeededResources(const NScheduler::TJobResources& value)
{
    NConcurrency::TWriterGuard guard(NeededResourcesLock_);
    NeededResources_ = value;
}

TJobResourcesWithQuotaList TOperationRuntimeData::GetMinNeededJobResources() const
{
    NConcurrency::TReaderGuard guard(MinNeededResourcesJobLock_);
    return MinNeededJobResources_;
}

void TOperationRuntimeData::SetMinNeededJobResources(const TJobResourcesWithQuotaList& value)
{
    NConcurrency::TWriterGuard guard(MinNeededResourcesJobLock_);
    MinNeededJobResources_ = value;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

