#pragma once

#include "private.h"
#include "operation_description.h"

#include <yt/yt/server/scheduler/public.h>
#include <yt/yt/server/scheduler/operation.h>

namespace NYT::NSchedulerSimulator {

////////////////////////////////////////////////////////////////////////////////

class TOperation
    : public TRefCounted
    , public NScheduler::IOperationStrategyHost
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
    std::optional<NScheduler::EUnschedulableReason> CheckUnschedulable(const std::optional<TString>& treeId) const override;
    TInstant GetStartTime() const override;
    TString GetAuthenticatedUser() const override;

    std::optional<int> FindSlotIndex(const TString& treeId) const override;
    void SetSlotIndex(const TString& treeId, int index) override;
    void ReleaseSlotIndex(const TString& treeId) override;

    NScheduler::IOperationControllerStrategyHostPtr GetControllerStrategyHost() const override;

    NScheduler::TStrategyOperationSpecPtr GetStrategySpec() const override;
    NScheduler::TStrategyOperationSpecPtr GetStrategySpecForTree(const TString& treeId) const override;

    const NYson::TYsonString& GetSpecString() const override;
    const NYson::TYsonString& GetTrimmedAnnotations() const override;

    NScheduler::TOperationRuntimeParametersPtr GetRuntimeParameters() const override;

    bool IsTreeErased(const TString& treeId) const override;

    void EraseTrees(const std::vector<TString>& treeIds) override;

    std::optional<NScheduler::TJobResources> GetAggregatedInitialMinNeededResources() const override;

    bool SetCompleting();

    void SetState(NScheduler::EOperationState state);

private:
    std::atomic<bool> Completing_ = {false};

    const NScheduler::TOperationId Id_;
    const NScheduler::EOperationType Type_;
    const NYson::TYsonString SpecString_;
    const NYson::TYsonString TrimmedAnnotations_;
    const TString AuthenticatedUser_;
    const TInstant StartTime_;
    const NScheduler::TOperationRuntimeParametersPtr RuntimeParameters_;
    NScheduler::EOperationState State_ = NScheduler::EOperationState::Running;
    THashMap<TString, int> TreeIdToSlotIndex_;
};

DEFINE_REFCOUNTED_TYPE(TOperation)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSchedulerSimulator
