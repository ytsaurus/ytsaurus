#pragma once

#include <library/cpp/yt/memory/ref.h>

namespace NYT::NWebAssembly {

////////////////////////////////////////////////////////////////////////////////

struct IWebAssemblyCompartment
    : public TNonCopyable
{
    virtual ~IWebAssemblyCompartment() = default;

    //! Adds new module in WebAssembly bytecode format.
    virtual void AddModule(TRef bytecode, TStringBuf name = "") = 0;
    //! Adds new module in WebAssembly WAST format.
    virtual void AddModule(const TString& wast, TStringBuf name = "") = 0;

    //! Strips compartment internal data structures.
    //! Stripped compartments can not be used for linking, but are faster to clone.
    virtual void Strip() = 0;

    //! Returns an opaque pointer to the function with name |name|.
    virtual void* GetFunction(const TString& name) = 0;
    //! Returns an opaque pointer to the function with index |index|.
    virtual void* GetFunction(size_t index) = 0;
    //! Returns an opaque pointer to the execution context.
    virtual void* GetContext() = 0;

    //! Converts compartment offset to host pointer and checks boundaries.
    virtual void* GetHostPointer(uintptr_t offset, size_t length) = 0;
    //! Converts host pointer to compartment offset.
    virtual uintptr_t GetCompartmentOffset(void* hostAddress) = 0;

    //! Clones compartment.
    //! NB: Cloning is slow and should not be called too often.
    virtual std::unique_ptr<IWebAssemblyCompartment> Clone() const = 0;

    //! Allocates data inside of the compartment.
    virtual uintptr_t AllocateBytes(size_t length) = 0;
    //! Deallocates data.
    virtual void FreeBytes(uintptr_t offset) = 0;
};

std::unique_ptr<IWebAssemblyCompartment> CreateBaseImage();

////////////////////////////////////////////////////////////////////////////////

IWebAssemblyCompartment* GetCurrentCompartment();
void SetCurrentCompartment(IWebAssemblyCompartment* compartment);
bool HasCurrentCompartment();

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NWebAssembly
