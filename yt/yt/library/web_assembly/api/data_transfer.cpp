#include "data_transfer.h"

#include "pointer.h"

namespace NYT::NWebAssembly {

////////////////////////////////////////////////////////////////////////////////

TCopyGuard::TCopyGuard(IWebAssemblyCompartment* compartment, uintptr_t offset)
    : Compartment_(compartment)
    , CopiedOffset_(offset)
{ }

TCopyGuard::~TCopyGuard()
{
    if (Compartment_ != nullptr && CopiedOffset_ != 0) {
        Compartment_->FreeBytes(CopiedOffset_);
    }
}

TCopyGuard::TCopyGuard(TCopyGuard&& other)
{
    std::swap(Compartment_, other.Compartment_);
    std::swap(CopiedOffset_, other.CopiedOffset_);
}

TCopyGuard& TCopyGuard::operator=(TCopyGuard&& other)
{
    std::swap(Compartment_, other.Compartment_);
    std::swap(CopiedOffset_, other.CopiedOffset_);
    return *this;
}

uintptr_t TCopyGuard::GetCopiedOffset() const
{
    return CopiedOffset_;
}

////////////////////////////////////////////////////////////////////////////////

template <>
TCopyGuard CopyIntoCompartment(TStringBuf data, IWebAssemblyCompartment* compartment)
{
    auto offset = compartment->AllocateBytes(data.size());
    auto* destination = ConvertPointerFromWasmToHost(std::bit_cast<char*>(offset), data.size());
    ::memcpy(destination, data.data(), data.size());
    return {compartment, offset};
}

template <>
TCopyGuard CopyIntoCompartment(const std::vector<i64>& data, IWebAssemblyCompartment* compartment)
{
    auto byteLength = data.size() * sizeof(i64);
    auto offset = compartment->AllocateBytes(byteLength);
    auto* destination = ConvertPointerFromWasmToHost(std::bit_cast<char*>(offset), byteLength);
    ::memcpy(destination, data.data(), byteLength);
    return {compartment, offset};
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NWebAssembly
