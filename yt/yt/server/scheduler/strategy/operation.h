#pragma once

#include "public.h"

#include <yt/yt/server/scheduler/common/structs.h>

#include <yt/yt/server/lib/scheduler/structs.h>

#include <yt/yt/ytlib/scheduler/disk_resources.h>

#include <library/cpp/yt/memory/atomic_intrusive_ptr.h>

namespace NYT::NScheduler::NStrategy {

////////////////////////////////////////////////////////////////////////////////

// TODO(eshcherbin): Refactor operation persistent attributes storing interface.
struct TOperationPoolTreeAttributes
    : public NYTree::TYsonStructLite
{
    std::optional<int> SlotIndex;
    bool RunningInEphemeralPool;
    bool RunningInLightweightPool;

    REGISTER_YSON_STRUCT_LITE(TOperationPoolTreeAttributes);

    static void Register(TRegistrar);
};

////////////////////////////////////////////////////////////////////////////////

struct IOperation
    : public TRefCounted
{
    virtual EOperationType GetType() const = 0;

    virtual EOperationState GetState() const = 0;

    virtual std::optional<EUnschedulableReason> CheckUnschedulable(const std::optional<std::string>& treeId = std::nullopt) const = 0;

    virtual TInstant GetStartTime() const = 0;

    virtual std::optional<int> FindSlotIndex(const std::string& treeId) const = 0;
    virtual void SetSlotIndex(const std::string& treeId, int index) = 0;
    virtual void ReleaseSlotIndex(const std::string& treeId) = 0;

    virtual std::string GetAuthenticatedUser() const = 0;

    virtual std::optional<std::string> GetTitle() const = 0;

    virtual TOperationId GetId() const = 0;

    virtual ISchedulingOperationControllerPtr GetControllerStrategyHost() const = 0;

    virtual TStrategyOperationSpecPtr GetStrategySpec() const = 0;

    virtual TStrategyOperationSpecPtr GetStrategySpecForTree(const std::string& treeId) const = 0;

    virtual const NYson::TYsonString& GetSpecString() const = 0;

    virtual const NYson::TYsonString& GetTrimmedAnnotations() const = 0;

    virtual const std::optional<TBriefVanillaTaskSpecMap>& GetMaybeBriefVanillaTaskSpecs() const = 0;

    virtual TOperationRuntimeParametersPtr GetRuntimeParameters() const = 0;

    virtual const TOperationOptionsPtr& GetOperationOptions() const = 0;

    virtual void UpdatePoolAttributes(
        const std::string& treeId,
        const TOperationPoolTreeAttributes& operationPoolTreeAttributes) = 0;

    virtual bool IsTreeErased(const std::string& treeId) const = 0;

    virtual void EraseTrees(const std::vector<std::string>& treeIds) = 0;
};

DEFINE_REFCOUNTED_TYPE(IOperation)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler::NStrategy
