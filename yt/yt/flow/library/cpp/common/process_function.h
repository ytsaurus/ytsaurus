#pragma once

#include "public.h"

#include <yt/yt/flow/library/cpp/common/input_context.h>
#include <yt/yt/flow/library/cpp/common/key.h>
#include <yt/yt/flow/library/cpp/common/message.h>
#include <yt/yt/flow/library/cpp/common/output_collector.h>
#include <yt/yt/flow/library/cpp/common/timer.h>
#include <yt/yt/flow/library/cpp/common/visit.h>

#include <yt/yt/flow/library/cpp/misc/public.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

//! Common base of a process function, decoupled from the computation and its stores. Carries only
//! the lifecycle hook; derive a granularity interface to declare how the epoch's input is handled.
struct IProcessFunctionBase
    : public TRefCounted
{
    //! Initializes the function once before processing begins; default no-op. |initContext|
    //! provides the state key clients and the static parameters.
    virtual void Init(const IRuntimeInitContextPtr& initContext);
};

DEFINE_REFCOUNTED_TYPE(IProcessFunctionBase)

////////////////////////////////////////////////////////////////////////////////

//! Process function with a hook per input entity; override the kinds it handles, all default
//! no-op. Hooks for entity kinds the host does not deliver never fire.
struct IProcessFunction
    : public IProcessFunctionBase
{
    //! Processes a single |message|; default no-op.
    virtual void ProcessMessage(
        const TInputMessageConstPtr& message,
        const IOutputCollectorPtr& output,
        const IRuntimeContextPtr& context);

    //! Processes a single |timer|; default no-op.
    virtual void ProcessTimer(
        const TInputTimerConstPtr& timer,
        const IOutputCollectorPtr& output,
        const IRuntimeContextPtr& context);

    //! Processes a single |visit|; default no-op.
    virtual void ProcessVisit(
        const TInputVisitConstPtr& visit,
        const IOutputCollectorPtr& output,
        const IRuntimeContextPtr& context);
};

DEFINE_REFCOUNTED_TYPE(IProcessFunction)

////////////////////////////////////////////////////////////////////////////////

//! Process function that handles the whole epoch's input in one call (e.g. a single batched
//! external request). Input is not grouped by key. This is the form the worker drives directly.
struct IBatchProcessFunction
    : public IProcessFunctionBase
{
    //! Processes the whole epoch's |input|, emitting to |output|; |context| exposes the per-call
    //! runtime accessors. Default no-op.
    virtual void Process(
        const IInputContextPtr& input,
        const IOutputCollectorPtr& output,
        const IRuntimeContextPtr& context);
};

DEFINE_REFCOUNTED_TYPE(IBatchProcessFunction)

////////////////////////////////////////////////////////////////////////////////

//! Process function that handles one group-by key at a time.
struct IKeyedBatchProcessFunction
    : public IProcessFunctionBase
{
    //! Processes one key's |input| (its messages, timers and visits together); default no-op.
    virtual void ProcessKey(
        const IInputContextPtr& input,
        const IOutputCollectorPtr& output,
        const IRuntimeContextPtr& context);
};

DEFINE_REFCOUNTED_TYPE(IKeyedBatchProcessFunction)

////////////////////////////////////////////////////////////////////////////////

//! Capability mix-in: a function commits end-of-epoch side effects by also deriving this. The
//! host obtains the mix-in through the registry (the opt-in is detected at registration), and a
//! host without a sync phase rejects such a function at spec validation.
struct ISyncProcessFunction
{
    virtual ~ISyncProcessFunction() = default;

    //! Commits side effects in |transaction| at the end of the epoch; |context| exposes the
    //! runtime accessors.
    virtual void Sync(const IRetryableTransactionPtr& transaction, const IRuntimeContextPtr& context) = 0;
};

////////////////////////////////////////////////////////////////////////////////

//! Adapts |function| of any granularity to the whole-epoch batch form the worker runs: a batch
//! function as-is, a per-element function as a per-entity dispatch, a per-key function as a
//! group-by-key dispatch.
IBatchProcessFunctionPtr WrapAsBatch(const IProcessFunctionBasePtr& function);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
