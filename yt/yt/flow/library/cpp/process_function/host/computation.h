#pragma once

#include "computation_runtime_context.h"
#include "runtime_init_context.h"

#include <yt/yt/flow/library/cpp/computation/swift_map_computation.h>
#include <yt/yt/flow/library/cpp/computation/transform_computation.h>

#include <yt/yt/flow/library/cpp/common/process_function.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

//! Instantiates the process function named by |spec|->ProcessingFunction (via TRegistry).
IProcessFunctionBasePtr CreateProcessFunction(const TComputationSpecPtr& spec);

//! Returns |function|'s sync mix-in if it opted in, else null (no RTTI; resolved via TRegistry).
ISyncProcessFunction* ViewProcessFunctionAsSync(const TComputationSpecPtr& spec, const IProcessFunctionBasePtr& function);

////////////////////////////////////////////////////////////////////////////////

//! Keyed computation (parameterized by the transform or swift-map worker base) that runs the
//! process function named by the spec's `processing_function` field, forwarding its parameters
//! through the init and runtime contexts.
template <class TBase>
class TProcessFunctionComputationBase
    : public TBase
{
public:
    TProcessFunctionComputationBase(
        TComputationContextPtr context,
        TDynamicComputationContextPtr dynamicContext);

    void DoInit(IJobInitContextPtr initContext) override;
    void DoProcess(IInputContextPtr input, IOutputCollectorPtr output) override;

protected:
    const IProcessFunctionBasePtr Function_;
    //! Function_ normalized to the whole-epoch batch form the worker drives.
    const IBatchProcessFunctionPtr Batch_;
    const TComputationRuntimeContextPtr RuntimeContext_;
};

////////////////////////////////////////////////////////////////////////////////

//! Transform-mode adapter: full exactly-once loop with a sync phase. A named concrete class
//! (not a type alias) so it registers under the clean TypeName "NYT::NFlow::TProcessFunctionComputation".
class TProcessFunctionComputation
    : public TProcessFunctionComputationBase<TTransformComputation>
{
public:
    static constexpr bool RequiresProcessingFunction = true;
    static constexpr bool InvokesProcessFunctionSync = true;

    TProcessFunctionComputation(
        TComputationContextPtr context,
        TDynamicComputationContextPtr dynamicContext)
        : TProcessFunctionComputationBase(std::move(context), std::move(dynamicContext))
    { }

    void DoSync(IRetryableTransactionPtr transaction) override;

private:
    //! Function_'s sync interface, resolved once; null when it needs no sync phase.
    ISyncProcessFunction* const SyncFunction_ = ViewProcessFunctionAsSync(this->GetSpec(), Function_);
};

//! Swift-map-mode adapter: lightweight no-sync loop.
class TProcessFunctionSwiftMapComputation
    : public TProcessFunctionComputationBase<TSwiftMapComputation>
{
public:
    static constexpr bool RequiresProcessingFunction = true;

    TProcessFunctionSwiftMapComputation(
        TComputationContextPtr context,
        TDynamicComputationContextPtr dynamicContext)
        : TProcessFunctionComputationBase(std::move(context), std::move(dynamicContext))
    { }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
