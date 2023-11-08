#include "compartment.h"

namespace NYT::NWebAssembly {

////////////////////////////////////////////////////////////////////////////////

Y_WEAK std::unique_ptr<IWebAssemblyCompartment> CreateBaseImage()
{
    YT_UNIMPLEMENTED();
}

Y_WEAK IWebAssemblyCompartment* GetCurrentCompartment()
{
    YT_UNIMPLEMENTED();
}

Y_WEAK void SetCurrentCompartment(IWebAssemblyCompartment*)
{
    YT_UNIMPLEMENTED();
}

Y_WEAK bool HasCurrentCompartment()
{
    YT_UNIMPLEMENTED();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NWebAssembly
