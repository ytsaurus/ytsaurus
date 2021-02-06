#pragma once

#include "private.h"
#include "operation_description.h"

#include <yt/server/scheduler/public.h>
#include <yt/server/scheduler/operation.h>

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

    virtual NScheduler::TOperationId GetId() const override;
    virtual NScheduler::EOperationType GetType() const override;
    virtual NScheduler::EOperationState GetState() const override;
    std::optional<NScheduler::EUnschedulableReason> CheckUnschedulable() const override;
    virtual TInstant GetStartTime() const override;
    virtual TString GetAuthenticatedUser() const override;

    virtual std::optional<int> FindSlotIndex(const TString& treeId) const override;
    virtual int GetSlotIndex(const TString& treeId) const override;
    virtual void SetSlotIndex(const TString&  treeId, int index) override;

    virtual NScheduler::IOperationControllerStrategyHostPtr GetControllerStrategyHost() const override;

    virtual NScheduler::TStrategyOperationSpecPtr GetStrategySpec() const override;
    virtual NScheduler::TStrategyOperationSpecPtr GetStrategySpecForTree(const TString& treeId) const override;

    virtual const NYson::TYsonString& GetSpecString() const override;

    virtual NScheduler::TOperationRuntimeParametersPtr GetRuntimeParameters() const override;

    virtual bool IsTreeErased(const TString& treeId) const override;
    
    virtual void EraseTrees(const std::vector<TString>& treeIds) override;

    virtual std::optional<NScheduler::TJobResources> GetInitialAggregatedMinNeededResources() const override;

    bool SetCompleting();
    
    void SetState(NScheduler::EOperationState state);

private:
    std::atomic<bool> Completing_ = {false};

    const NScheduler::TOperationId Id_;
    const NScheduler::EOperationType Type_;
    const NYson::TYsonString SpecString_;
    const TString AuthenticatedUser_;
    const TInstant StartTime_;
    const NScheduler::TOperationRuntimeParametersPtr RuntimeParameters_;
    NScheduler::EOperationState State_ = NScheduler::EOperationState::Running;
    THashMap<TString, int> TreeIdToSlotIndex_;
};

DEFINE_REFCOUNTED_TYPE(TOperation)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSchedulerSimulator
