#include "operation.h"
#include "exec_node.h"
#include "helpers.h"
#include "job.h"

#include <yt/server/controller_agent/operation_controller.h>

#include <yt/ytlib/scheduler/helpers.h>
#include <yt/ytlib/scheduler/config.h>

namespace NYT {
namespace NScheduler {

using namespace NApi;
using namespace NTransactionClient;
using namespace NJobTrackerClient;
using namespace NRpc;
using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

TOperation::TOperation(
    const TOperationId& id,
    EOperationType type,
    const TMutationId& mutationId,
    TTransactionId userTransactionId,
    IMapNodePtr spec,
    const TString& authenticatedUser,
    const std::vector<TString>& owners,
    TInstant startTime,
    IInvokerPtr controlInvoker,
    EOperationState state,
    bool suspended,
    const std::vector<TOperationEvent>& events)
    : Id_(id)
    , Type_(type)
    , MutationId_(mutationId)
    , State_(state)
    , Suspended_(suspended)
    , Activated_(false)
    , Prepared_(false)
    , UserTransactionId_(userTransactionId)
    , Spec_(spec)
    , RuntimeParams_(New<TOperationRuntimeParams>())
    , AuthenticatedUser_(authenticatedUser)
    , Owners_(owners)
    , StartTime_(startTime)
    , Events_(events)
    , CodicilData_(MakeOperationCodicilString(Id_))
    , CancelableContext_(New<TCancelableContext>())
    , CancelableInvoker_(CancelableContext_->CreateInvoker(controlInvoker))
{
    auto parsedSpec = ConvertTo<TOperationSpecBasePtr>(Spec_);
    SecureVault_ = std::move(parsedSpec->SecureVault);
    Spec_->RemoveChild("secure_vault");

    RuntimeParams_->Weight = parsedSpec->Weight.Get(1.0);
    RuntimeParams_->ResourceLimits = parsedSpec->ResourceLimits;
    RuntimeParams_->Owners = parsedSpec->Owners;
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

void TOperation::UpdateControllerTimeStatistics(const TStatistics& statistics)
{
    ControllerTimeStatistics_.Update(statistics);
}

bool TOperation::HasControllerProgress() const
{
    return (State_ == EOperationState::Running || IsFinishedState()) &&
        Controller_ &&
        Controller_->HasProgress();
}

bool TOperation::HasControllerJobSplitterInfo() const
{
    return State_ == EOperationState::Running &&
        Controller_ &&
        Controller_->HasJobSplitterInfo();
}

TCodicilGuard TOperation::MakeCodicilGuard() const
{
    return TCodicilGuard(CodicilData_);
}

void TOperation::SetState(EOperationState state)
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

const yhash<TString, int>& TOperation::GetSlotIndices() const
{
    return TreeIdToSlotIndex_;
}

const IInvokerPtr& TOperation::GetCancelableControlInvoker()
{
    return CancelableInvoker_;
}

void TOperation::Cancel()
{
    CancelableContext_->Cancel();
}

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

} // namespace NScheduler
} // namespace NYT

