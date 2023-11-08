#include <yt/yt/library/web_assembly/api/compartment.h>

namespace NYT::NWebAssembly {

////////////////////////////////////////////////////////////////////////////////

class TWebAssemblyCompartment
    : public IWebAssemblyCompartment
{ };

std::unique_ptr<IWebAssemblyCompartment> CreateBaseImage()
{
    YT_UNIMPLEMENTED();
}

////////////////////////////////////////////////////////////////////////////////

static thread_local IWebAssemblyCompartment* CurrentCompartment;

IWebAssemblyCompartment* GetCurrentCompartment()
{
    return CurrentCompartment;
}

void SetCurrentCompartment(IWebAssemblyCompartment* compartment)
{
    CurrentCompartment = compartment;
}

bool HasCurrentCompartment()
{
    return CurrentCompartment != nullptr;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NWebAssembly
