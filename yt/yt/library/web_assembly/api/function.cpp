#include "function.h"

namespace NYT::NWebAssembly {

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

Y_WEAK void WavmInvoke(
    IWebAssemblyCompartment* /*compartment*/,
    TWebAssemblyRuntimeType /*runtimeType*/,
    TCompartmentFunctionId /*runtimeFunction*/,
    TWavmPodValue* /*result*/,
    TRange<TWavmPodValue> /*arguments*/)
{
    YT_UNIMPLEMENTED();
}

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NWebAssembly
