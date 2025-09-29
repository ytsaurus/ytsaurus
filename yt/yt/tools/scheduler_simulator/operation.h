#pragma once

#include "private.h"
#include "operation_description.h"

#include <yt/yt/server/scheduler/strategy/operation.h>

#include <yt/yt/server/scheduler/public.h>
#include <yt/yt/server/scheduler/operation.h>

namespace NYT::NSchedulerSimulator {

////////////////////////////////////////////////////////////////////////////////

class TOperation
    : public NScheduler::NStrategy::IOperation
{
public:
    DEFINE_BYVAL_RW_PROPERTY(ISimulatorOperationControllerPtr, Controller);

public:
    TOperation(
        const TOperationDescription& description,
        const NScheduler::TOperationRuntimeParametersPtr& runtimeParameters);

    NScheduler::TOperationId GetId() const override;
    NScheduler::EOperationType GetType() const override;
    NScheduler::EOperationState GetState() const override;
    std::optional<NScheduler::NStrategy::EUnschedulableReason> CheckUnschedulable(const std::optional<std::string>& treeId) const override;
    TInstant GetStartTime() const override;
    std::string GetAuthenticatedUser() const override;
    std::optional<std::string> GetTitle() const override;

    std::optional<int> FindSlotIndex(const std::string& treeId) const override;
    void SetSlotIndex(const std::string& treeId, int index) override;
    void ReleaseSlotIndex(const std::string& treeId) override;

    NScheduler::NStrategy::ISchedulingOperationControllerPtr GetControllerStrategyHost() const override;

    NScheduler::TStrategyOperationSpecPtr GetStrategySpec() const override;
    NScheduler::TStrategyOperationSpecPtr GetStrategySpecForTree(const std::string& treeId) const override;

    const NYson::TYsonString& GetSpecString() const override;
    const NYson::TYsonString& GetTrimmedAnnotations() const override;
    const std::optional<NScheduler::TBriefVanillaTaskSpecMap>& GetMaybeBriefVanillaTaskSpecs() const override;

    NScheduler::TOperationRuntimeParametersPtr GetRuntimeParameters() const override;

    void UpdatePoolAttributes(const std::string& /*treeId*/, const NScheduler::NStrategy::TOperationPoolTreeAttributes& /*operationPoolTreeAttributes*/) override;

    bool IsTreeErased(const std::string& treeId) const override;

    void EraseTrees(const std::vector<std::string>& treeIds) override;

    bool SetCompleting();

    void SetState(NScheduler::EOperationState state);

    const NScheduler::TOperationOptionsPtr& GetOperationOptions() const override;

private:
    std::atomic<bool> Completing_ = false;

    const NScheduler::TOperationId Id_;
    const NScheduler::EOperationType Type_;
    const NYson::TYsonString SpecString_;
    const std::optional<NScheduler::TBriefVanillaTaskSpecMap> BriefVanillaTaskSpecs_;
    const NYson::TYsonString TrimmedAnnotations_;
    const std::string AuthenticatedUser_;
    const TInstant StartTime_;
    const NScheduler::TOperationRuntimeParametersPtr RuntimeParameters_;
    NScheduler::EOperationState State_ = NScheduler::EOperationState::Running;
    THashMap<std::string, int> TreeIdToSlotIndex_;
};

DEFINE_REFCOUNTED_TYPE(TOperation)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSchedulerSimulator
