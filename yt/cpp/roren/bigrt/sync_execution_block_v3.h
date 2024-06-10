#pragma once

#include "bigrt_execution_context.h"

#include <yt/cpp/roren/interface/private/raw_par_do.h>  // IRawParDo
#include <yt/cpp/roren/interface/private/par_do_tree.h>  // TParDoTreeBuilder
#include <yt/cpp/roren/bigrt/stateful_timer_impl/stateful_timer_par_do.h>  // TStatefulTimerParDoWrapperPtr

#include <bigrt/lib/processing/shard_processor/stateless/processor.h>  // NBigRT::TStatelessShardProcessor::TRowWithMeta

namespace NRoren::NPrivate {

////////////////////////////////////////////////////////////////////////////////

class IRowWithMetaOutput
    : public IRawOutput
{
public:
    virtual void OnStartBundle(const IBigRtExecutionContextPtr& executionContext) = 0;
    virtual void OnRow() = 0;
    virtual void OnTimer() = 0;
    virtual void OnFinishBundle() = 0;
};  // class IRowWithMetaOutput

using IRowWithMetaOutputPtr = TIntrusivePtr<IRowWithMetaOutput>;

////////////////////////////////////////////////////////////////////////////////

class TSyncExecutionBlockV3
    : public TThrRefBase
{
public:
    TSyncExecutionBlockV3(IParDoTreePtr parDo, THashMap<TString, TCreateBaseStateManagerFunction> createBaseStateManagerFunctions, THashMap<TString, NPrivate::TStatefulTimerParDoWrapperPtr> timersCallbacks);
    void StartBundle(IBigRtExecutionContextPtr context, std::vector<IRowWithMetaOutputPtr> outputs);
    void Do(NBigRT::TStatelessShardProcessor::TRowWithMeta rowWithMeta) const;
    void OnTimer(const TString& callbackId, std::vector<TTimer> readyTimers) const;
    void FinishBundle() const;
    const THashMap<TString, NPrivate::TStatefulTimerParDoWrapperPtr>& GetTimersCallbacks() const noexcept;
    const THashMap<TString, TCreateBaseStateManagerFunction>& GetStateManagerFunctions() const noexcept;

    TRowVtable GetInputVtable() const noexcept;  //TODO: Return vector of vtable
    std::vector<TDynamicTypeTag> GetOutputTags() const noexcept;

    TString GetDebugDescription() const noexcept;

private:
    const IParDoTreePtr ParDo_;
    const THashMap<TString, TCreateBaseStateManagerFunction> CreateBaseStateManagerFunctions_;
    const THashMap<TString, TStatefulTimerParDoWrapperPtr> TimerCallbacks_;

    IBigRtExecutionContextPtr ExecutionContext_;
    std::vector<IRowWithMetaOutputPtr> Outputs_;
};  // class TSyncExecutionBlockV3

using TSyncExecutionBlockV3Ptr = TIntrusivePtr<TSyncExecutionBlockV3>;

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren::NPrivate
