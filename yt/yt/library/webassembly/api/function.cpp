#include "function.h"

namespace NYT::NWebAssembly {

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

Y_WEAK void WavmInvoke(
    IWebAssemblyCompartment*,
    TWebAssemblyRuntimeType,
    TCompartmentFunctionId,
    TWavmPodValue*,
    TRange<TWavmPodValue>)
{
    YT_UNIMPLEMENTED();
}

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NWebAssembly
