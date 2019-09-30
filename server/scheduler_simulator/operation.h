#pragma once

#include "private.h"
#include "operation_description.h"

#include <yt/server/scheduler/public.h>
#include <yt/server/scheduler/operation.h>

namespace NYT::NSchedulerSimulator {

////////////////////////////////////////////////////////////////////////////////

class TOperation
    : public TIntrinsicRefCounted
    , public NScheduler::IOperationStrategyHost
{
public:
    DEFINE_BYVAL_RW_PROPERTY(NScheduler::EOperationState, State);
    DEFINE_BYVAL_RW_PROPERTY(ISimulatorOperationControllerPtr, Controller);

public:
    TOperation(
        const TOperationDescription& description,
        const NScheduler::TOperationRuntimeParametersPtr& runtimeParameters);

    virtual NScheduler::TOperationId GetId() const override;
    virtual NScheduler::EOperationType GetType() const override;
    virtual bool IsSchedulable() const override;
    virtual TInstant GetStartTime() const override;
    virtual TString GetAuthenticatedUser() const override;

    virtual std::optional<int> FindSlotIndex(const TString& treeId) const override;
    virtual int GetSlotIndex(const TString& treeId) const override;
    virtual void SetSlotIndex(const TString&  treeId, int index) override;

    virtual NScheduler::IOperationControllerStrategyHostPtr GetControllerStrategyHost() const override;

    virtual const NYson::TYsonString& GetSpecString() const override;

    virtual NScheduler::TOperationRuntimeParametersPtr GetRuntimeParameters() const override;

    virtual bool GetActivated() const override;

    bool SetCompleting();

    void SetErasedTrees(std::vector<TString> erasedTrees) override;
    const std::vector<TString>& ErasedTrees() const override;

private:
    std::atomic<bool> Completing_ = {false};

    const NScheduler::TOperationId Id_;
    const NScheduler::EOperationType Type_;
    const NYson::TYsonString SpecString_;
    const TString AuthenticatedUser_;
    const TInstant StartTime_;
    const NScheduler::TOperationRuntimeParametersPtr RuntimeParameters_;
    THashMap<TString, int> TreeIdToSlotIndex_;

    std::vector<TString> ErasedTrees_;

    void EraseTree(const TString& treeId) override;
};

DEFINE_REFCOUNTED_TYPE(TOperation)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSchedulerSimulator
