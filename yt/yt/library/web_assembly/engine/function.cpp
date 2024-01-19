#include "wavm_private_imports.h"

#include <yt/yt/library/web_assembly/api/function.h>

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
            std::bit_cast<Uptr>(type)});

    Runtime::invokeFunction(
        static_cast<Runtime::Context*>(compartment->GetContext()),
        static_cast<Runtime::Function*>(runtimeFunction),
        wavmType,
        std::bit_cast<IR::UntaggedValue*>(arguments.Begin()),
        std::bit_cast<IR::UntaggedValue*>(result));
}

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NWebAssembly
