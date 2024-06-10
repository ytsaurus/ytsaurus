#include "sync_execution_block_v3.h"

#include "bigrt.h"
#include "bigrt_execution_context.h"

#include <util/generic/guid.h>
#include <yt/cpp/roren/library/logger/logger.h>

USE_ROREN_LOGGER();

namespace NRoren::NPrivate {

////////////////////////////////////////////////////////////////////////////////
template <typename TMethod, class... TArgs>
void InvokeOutputsMethod(const std::vector<IRowWithMetaOutputPtr>& outputs, TMethod method, const TArgs& ...args)
{
    Y_UNUSED(outputs, method, args...);
    for (auto& output : outputs) {
        (output.Get()->*method)(args...);
    }
}

TSyncExecutionBlockV3::TSyncExecutionBlockV3(IParDoTreePtr parDo, THashMap<TString, TCreateBaseStateManagerFunction> createBaseStateManagerFunctions, THashMap<TString, NPrivate::TStatefulTimerParDoWrapperPtr> timersCallbacks)
    : ParDo_(std::move(parDo))
    , CreateBaseStateManagerFunctions_(std::move(createBaseStateManagerFunctions))
    , TimerCallbacks_(std::move(timersCallbacks))
{
}

void TSyncExecutionBlockV3::StartBundle(IBigRtExecutionContextPtr context, std::vector<IRowWithMetaOutputPtr> outputs)
{
    ExecutionContext_ = context;
    std::vector<IRawOutputPtr> downgradedOutputs;
    downgradedOutputs.reserve(outputs.size());
    for (auto& output : outputs) {
        downgradedOutputs.push_back(output);
    }
    Outputs_ = std::move(outputs);

    InvokeOutputsMethod(Outputs_, &IRowWithMetaOutput::OnStartBundle, context);
    ParDo_->Start(std::move(context), std::move(downgradedOutputs));
}

void TSyncExecutionBlockV3::Do(NBigRT::TStatelessShardProcessor::TRowWithMeta rowWithMeta) const
{
    Y_ABORT_IF(GetInputVtable().TypeHash != rowWithMeta.ValueTypeHash);
    TBigRtExecutionContextOps::SetRowMeta(ExecutionContext_, rowWithMeta);
    InvokeOutputsMethod(Outputs_, &IRowWithMetaOutput::OnRow);
    ParDo_->Do(rowWithMeta.ValuePtr.get(), 1);
}

void TSyncExecutionBlockV3::OnTimer(const TString& callbackId, std::vector<TTimer> readyTimers) const
{
    InvokeOutputsMethod(Outputs_, &IRowWithMetaOutput::OnTimer);
    TimerCallbacks_.at(callbackId)->OnTimer(std::move(readyTimers));
}

void TSyncExecutionBlockV3::FinishBundle() const
{
    InvokeOutputsMethod(Outputs_, &IRowWithMetaOutput::OnFinishBundle);
    ParDo_->Finish();
}

const THashMap<TString, NPrivate::TStatefulTimerParDoWrapperPtr>& TSyncExecutionBlockV3::GetTimersCallbacks() const noexcept
{
    return TimerCallbacks_;
}

const THashMap<TString, TCreateBaseStateManagerFunction>& TSyncExecutionBlockV3::GetStateManagerFunctions() const noexcept
{
    return CreateBaseStateManagerFunctions_;
}

TRowVtable TSyncExecutionBlockV3::GetInputVtable() const noexcept
{
    auto tags = ParDo_->GetInputTags();
    YT_LOG_FATAL_IF(tags.size() != 1, "Block must have exactly 1 input, have %v", tags.size());
    return tags[0].GetRowVtable();
}

std::vector<TDynamicTypeTag> TSyncExecutionBlockV3::GetOutputTags() const noexcept
{
    return ParDo_->GetOutputTags();
}

TString TSyncExecutionBlockV3::GetDebugDescription() const noexcept
{
    return ParDo_->GetDebugDescription();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren::NPrivate
