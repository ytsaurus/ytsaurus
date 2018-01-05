#include "operation.h"
#include "exec_node.h"
#include "helpers.h"
#include "job.h"

#include <yt/server/controller_agent/operation_controller.h>

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

TOperation::TOperation(
    const TOperationId& id,
    EOperationType type,
    const TMutationId& mutationId,
    const TTransactionId& userTransactionId,
    IMapNodePtr spec,
    IMapNodePtr secureVault,
    TOperationRuntimeParamsPtr runtimeParams,
    const TString& authenticatedUser,
    const std::vector<TString>& owners,
    TInstant startTime,
    IInvokerPtr controlInvoker,
    EOperationCypressStorageMode storageMode,
    EOperationState state,
    bool suspended,
    const std::vector<TOperationEvent>& events,
    const TNullable<TOperationRevivalDescriptor>& revivalDescriptor)
    : Type_(type)
    , MutationId_(mutationId)
    , State_(state)
    , Suspended_(suspended)
    , UserTransactionId_(userTransactionId)
    , RuntimeParams_(std::move(runtimeParams))
    , SecureVault_(std::move(secureVault))
    , Owners_(owners)
    , Events_(events)
    , RevivalDescriptor_(revivalDescriptor)
    , StorageMode_(storageMode)
    , Id_(id)
    , StartTime_(startTime)
    , AuthenticatedUser_(authenticatedUser)
    , Spec_(spec)
    , CodicilData_(MakeOperationCodicilString(Id_))
    , CancelableContext_(New<TCancelableContext>())
    , CancelableInvoker_(CancelableContext_->CreateInvoker(controlInvoker))
{ }

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

