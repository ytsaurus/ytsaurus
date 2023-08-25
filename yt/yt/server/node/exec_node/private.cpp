#include "private.h"

#include <library/cpp/yt/string/format.h>
#include <library/cpp/yt/string/guid.h>

namespace NYT::NExecNode {

////////////////////////////////////////////////////////////////////////////////

bool TControllerAgentDescriptor::operator==(const TControllerAgentDescriptor& other) const noexcept
{
    return other.Address == Address && other.IncarnationId == IncarnationId;
}

bool TControllerAgentDescriptor::operator!=(const TControllerAgentDescriptor& other) const noexcept
{
    return !(*this == other);
}

bool TControllerAgentDescriptor::Empty() const noexcept
{
    return *this == TControllerAgentDescriptor{};
}

TControllerAgentDescriptor::operator bool() const noexcept
{
    return !Empty();
}

////////////////////////////////////////////////////////////////////////////////

void FormatValue(
    TStringBuilderBase* builder,
    const TControllerAgentDescriptor& controllerAgentDescriptor,
    TStringBuf /*format*/)
{
    builder->AppendFormat(
        "{Address: %v, IncarnationId: %v}",
        controllerAgentDescriptor.Address,
        controllerAgentDescriptor.IncarnationId);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode

size_t THash<NYT::NExecNode::TControllerAgentDescriptor>::operator () (
    const NYT::NExecNode::TControllerAgentDescriptor& descriptor) const
{
    size_t hash = THash<decltype(descriptor.Address)>{}(descriptor.Address);
    NYT::HashCombine(hash, descriptor.IncarnationId);

    return hash;
}
