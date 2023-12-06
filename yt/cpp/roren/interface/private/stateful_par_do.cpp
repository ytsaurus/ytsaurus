#include "stateful_par_do.h"

namespace NRoren::NPrivate {

////////////////////////////////////////////////////////////////////////////////

TLambdaStatefulParDo::TLambdaStatefulParDo(
    TWrapperFunction* wrapperFunction,
    void* underlyingFunction,
    TRowVtable inputRowVtable,
    std::vector<TDynamicTypeTag> outputTags,
    TRowVtable stateVtable,
    TFnAttributes fnAttributes)
    : WrapperFunction_(wrapperFunction)
    , UnderlyingFunction_(underlyingFunction)
    , InputTag_("input", inputRowVtable)
    , OutputTags_(std::move(outputTags))
    , StateVtable_(stateVtable)
    , FnAttributes_(fnAttributes)
{}

void TLambdaStatefulParDo::Start(const IExecutionContextPtr&, IRawStateStorePtr stateStore, const std::vector<IRawOutputPtr>& outputs)
{
    StateStore_ = stateStore;
    if (outputs.size() > 0) {
        Y_ABORT_UNLESS(outputs.size() == 1);

        SingleOutput_ = outputs[0];
    }
}

void TLambdaStatefulParDo::Do(const void* rows, int count)
{
    WrapperFunction_(this, rows, count);
}

void TLambdaStatefulParDo::Finish()
{
    SingleOutput_ = nullptr;
}

ISerializable<IRawStatefulParDo>::TDefaultFactoryFunc TLambdaStatefulParDo::GetDefaultFactory() const
{
    return [] () -> IRawStatefulParDoPtr {
        return ::MakeIntrusive<TLambdaStatefulParDo>();
    };
}

TRowVtable TLambdaStatefulParDo::GetStateVtable() const
{
    return StateVtable_;
}

const TFnAttributes& TLambdaStatefulParDo::GetFnAttributes() const
{
    return FnAttributes_;
}

std::vector<TDynamicTypeTag> TLambdaStatefulParDo::GetOutputTags() const
{
    return OutputTags_;
}

std::vector<TDynamicTypeTag> TLambdaStatefulParDo::GetInputTags() const
{
    return std::vector{InputTag_};
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren::NPrivate
