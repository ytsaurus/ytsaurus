#include "wavm_private_imports.h"

#include <yt/yt/library/web_assembly/api/function.h>

#include <yt/yt/library/numeric/util.h>

namespace NYT::NWebAssembly {

using namespace WAVM;

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

void WavmInvoke(
    IWebAssemblyCompartment* compartment,
    TWebAssemblyRuntimeType type,
    TCompartmentFunctionId runtimeFunction,
    TWavmPodValue* result,
    TRange<TWavmPodValue> arguments)
{
    const auto wavmType = IR::FunctionType(
        IR::FunctionType::Encoding{
            BitCast<Uptr>(type)});

    Runtime::invokeFunction(
        static_cast<Runtime::Context*>(compartment->GetContext()),
        static_cast<Runtime::Function*>(runtimeFunction),
        wavmType,
        BitCast<IR::UntaggedValue*>(arguments.Begin()),
        BitCast<IR::UntaggedValue*>(result));
}

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NWebAssembly
