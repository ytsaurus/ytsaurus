#include "operation.h"
#include "operation_controller.h"


namespace NYT::NSchedulerSimulator {

////////////////////////////////////////////////////////////////////////////////

TOperation::TOperation(
    const TOperationDescription& description,
    const NScheduler::TOperationRuntimeParametersPtr& runtimeParameters)
    : Id_(description.Id)
    , Type_(description.Type)
    , SpecString_(description.Spec)
    , AuthenticatedUser_(description.AuthenticatedUser)
    , StartTime_(description.StartTime)
    , RuntimeParams_(runtimeParameters)
{ }

NScheduler::TOperationId TOperation::GetId() const
{
    return Id_;
}

NScheduler::EOperationType TOperation::GetType() const
{
    return Type_;
}

bool TOperation::IsSchedulable() const
{
    return true;
}

TInstant TOperation::GetStartTime() const
{
    return StartTime_;
}

TString TOperation::GetAuthenticatedUser() const
{
    return AuthenticatedUser_;
}

void TOperation::SetSlotIndex(const TString& treeId, int value)
{
    TreeIdToSlotIndex_.emplace(treeId, value);
}

std::optional<int> TOperation::FindSlotIndex(const TString& treeId) const
{
    auto it = TreeIdToSlotIndex_.find(treeId);
    return it != TreeIdToSlotIndex_.end() ? std::make_optional(it->second) : std::nullopt;
}

int TOperation::GetSlotIndex(const TString& treeId) const
{
    auto slotIndex = FindSlotIndex(treeId);
    YT_VERIFY(slotIndex);
    return *slotIndex;
}

NScheduler::IOperationControllerStrategyHostPtr TOperation::GetControllerStrategyHost() const
{
    return Controller_;
}

const NYson::TYsonString& TOperation::GetSpecString() const
{
    return SpecString_;
}

NScheduler::TOperationRuntimeParametersPtr TOperation::GetRuntimeParameters() const
{
    return RuntimeParams_;
}

bool TOperation::SetCompleting()
{
    return !Completing_.exchange(true);
}

bool TOperation::GetActivated() const
{
    // NB(renadeen): return value doesn't matter in simulator.
    return true;
}

void TOperation::SetErasedTrees(std::vector<TString> erasedTrees)
{
    ErasedTrees_ = std::move(erasedTrees);
}

const std::vector<TString>& TOperation::ErasedTrees() const
{
    return ErasedTrees_;
}

void TOperation::EraseTree(const TString& treeId)
{
    ErasedTrees_.push_back(treeId);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSchedulerSimulator
