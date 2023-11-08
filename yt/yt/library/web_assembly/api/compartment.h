#pragma once

#include <library/cpp/yt/memory/ref.h>

namespace NYT::NWebAssembly {

////////////////////////////////////////////////////////////////////////////////

struct IWebAssemblyCompartment
    : public TNonCopyable
{
    virtual ~IWebAssemblyCompartment() = default;

    //! Returns an opaque pointer to the function with index |index|.
    virtual void* GetFunction(size_t index) = 0;
};

std::unique_ptr<IWebAssemblyCompartment> CreateBaseImage();

////////////////////////////////////////////////////////////////////////////////

IWebAssemblyCompartment* GetCurrentCompartment();
void SetCurrentCompartment(IWebAssemblyCompartment* compartment);
bool HasCurrentCompartment();

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NWebAssembly
