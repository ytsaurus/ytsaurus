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
    , RuntimeParameters_(runtimeParameters)
{ }

NScheduler::TOperationId TOperation::GetId() const
{
    return Id_;
}

NScheduler::EOperationType TOperation::GetType() const
{
    return Type_;
}

NScheduler::EOperationState TOperation::GetState() const
{
    return State_;
}

std::optional<NScheduler::EUnschedulableReason> TOperation::CheckUnschedulable() const
{
    if (Controller_->GetPendingJobCount() == 0) {
        return NScheduler::EUnschedulableReason::NoPendingJobs;
    }

    return std::nullopt;
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
    TreeIdToSlotIndex_[treeId] = value;
}

void TOperation::ReleaseSlotIndex(const TString& treeId)
{
    YT_VERIFY(TreeIdToSlotIndex_.erase(treeId) == 1);
}

std::optional<int> TOperation::FindSlotIndex(const TString& treeId) const
{
    auto it = TreeIdToSlotIndex_.find(treeId);
    return it != TreeIdToSlotIndex_.end() ? std::make_optional(it->second) : std::nullopt;
}

NScheduler::IOperationControllerStrategyHostPtr TOperation::GetControllerStrategyHost() const
{
    return Controller_;
}

NScheduler::TStrategyOperationSpecPtr TOperation::GetStrategySpec() const
{
    try {
        return NYTree::ConvertTo<NScheduler::TStrategyOperationSpecPtr>(GetSpecString());
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Error parsing strategy spec of operation")
                << ex;
    }
}

NScheduler::TStrategyOperationSpecPtr TOperation::GetStrategySpecForTree(const TString& treeId) const
{
    auto spec = GetStrategySpec();
    auto optionsIt = spec->SchedulingOptionsPerPoolTree.find(treeId);
    if (optionsIt != spec->SchedulingOptionsPerPoolTree.end()) {
        spec = UpdateYsonSerializable(spec, ConvertToNode(optionsIt->second));
    }
    return spec;
}

const NYson::TYsonString& TOperation::GetSpecString() const
{
    return SpecString_;
}

NScheduler::TOperationRuntimeParametersPtr TOperation::GetRuntimeParameters() const
{
    return RuntimeParameters_;
}

bool TOperation::SetCompleting()
{
    return !Completing_.exchange(true);
}

void TOperation::SetState(NScheduler::EOperationState state)
{
    State_ = state;
}

bool TOperation::IsTreeErased(const TString& /*treeId*/) const
{
    YT_UNIMPLEMENTED();
}

void TOperation::EraseTrees(const std::vector<TString>& /*treeIds*/)
{
    YT_UNIMPLEMENTED();
}

std::optional<NScheduler::TJobResources> TOperation::GetInitialAggregatedMinNeededResources() const
{
    return std::nullopt;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSchedulerSimulator
