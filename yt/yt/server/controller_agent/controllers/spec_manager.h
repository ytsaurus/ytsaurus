#pragma once

#include "private.h"

#include <yt/yt/ytlib/scheduler/config.h>

#include <yt/yt/library/safe_assert/safe_assert.h>

#include <yt/yt/core/ytree/yson_struct_update.h>


namespace NYT::NControllerAgent::NControllers {

////////////////////////////////////////////////////////////////////////////////

using TOperationSpecBaseConfigurator = NYTree::TConfigurator<NScheduler::TOperationSpecBase>;
using TOperationSpecBaseSealedConfigurator = NYTree::TSealedConfigurator<NScheduler::TOperationSpecBase>;

struct ISpecManagerHost
{
    virtual TOperationSpecBaseSealedConfigurator ConfigureUpdate() = 0;
    virtual TOperationSpecBasePtr ParseTypedSpec(const NYTree::INodePtr& spec) const = 0;

    virtual std::any CreateSafeAssertionGuard() const = 0;
    virtual void ProcessSafeException(const TAssertionFailedException& ex) = 0;
    virtual void ProcessSafeException(const std::exception& ex) = 0;
};

////////////////////////////////////////////////////////////////////////////////

class TSpecManager
    : public TRefCounted
{
public:
    TSpecManager(
        ISpecManagerHost* host,
        NScheduler::TOperationSpecBasePtr spec,
        NLogging::TLogger logger);

    //! NB: Do not reuse |cumulativeSpecPatch| after call.
    void InitializeReviving(NYTree::INodePtr&& cumulativeSpecPatch);
    void InitializeClean();

    template <class TSpec = NScheduler::TOperationSpecBase>
    TIntrusivePtr<TSpec> GetSpec() const;

    void ValidateSpecPatch(const NYTree::INodePtr& newCumulativeSpecPatch) const;
    void ApplySpecPatch(NYTree::INodePtr newCumulativeSpecPatch);

    void ApplySpecPatchReviving();

private:
    //! |shouldFail| is a testing mode switch indicating whether apply should throw exception.
    void DoApply(
        const NScheduler::TOperationSpecBasePtr& currentSpec,
        NYTree::INodePtr patch,
        bool shouldFail);

    ISpecManagerHost* Host_;
    NScheduler::TPatchSpecProtocolTestingOptionsPtr TestingSpec_;
    NScheduler::TOperationSpecBasePtr OriginalSpec_;
    TAtomicIntrusivePtr<NScheduler::TOperationSpecBase> DynamicSpec_;
    std::optional<TOperationSpecBaseSealedConfigurator> UpdateConfigurator_;
    NYTree::INodePtr InitialCumulativeSpecPatch_;
    NLogging::TLogger Logger;
};

DEFINE_REFCOUNTED_TYPE(TSpecManager)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent::NControllers

#define SPEC_MANAGER_INL_H
#include "spec_manager-inl.h"
#undef SPEC_MANAGER_INL_H
