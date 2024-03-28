#include "compartment.h"

namespace NYT::NWebAssembly {

////////////////////////////////////////////////////////////////////////////////

Y_WEAK std::unique_ptr<IWebAssemblyCompartment> CreateEmptyImage()
{
    YT_UNIMPLEMENTED();
}

Y_WEAK std::unique_ptr<IWebAssemblyCompartment> CreateStandardRuntimeImage()
{
    YT_UNIMPLEMENTED();
}

Y_WEAK std::unique_ptr<IWebAssemblyCompartment> CreateQueryLanguageImage()
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
