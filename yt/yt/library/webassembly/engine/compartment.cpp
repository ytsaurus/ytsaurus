#include <yt/yt/library/webassembly/api/compartment.h>

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

static thread_local TWebAssemblyCompartment* CurrentCompartment = nullptr;

IWebAssemblyCompartment* GetCurrentCompartment()
{
    return CurrentCompartment;
}

void SetCurrentCompartment(IWebAssemblyCompartment* compartment)
{
    CurrentCompartment = static_cast<TWebAssemblyCompartment*>(compartment);
}

bool HasCurrentCompartment()
{
    return CurrentCompartment != nullptr;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NWebAssembly
