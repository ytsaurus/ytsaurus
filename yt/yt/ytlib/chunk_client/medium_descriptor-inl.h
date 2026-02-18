#ifndef MEDIUM_DESCRIPTOR_INL_H_
#error "Direct inclusion of this file is not allowed, include chunk.h"
// For the sake of sane code completion.
#include "medium_descriptor.h"
#endif

namespace NYT::NChunkClient {

////////////////////////////////////////////////////////////////////////////////

template <class TDerived>
requires std::derived_from<std::remove_cvref_t<TDerived>, TMediumDescriptor>
TIntrusivePtr<TDerived> TMediumDescriptor::As()
{
    return static_cast<TDerived*>(this);
}

template <class TDerived>
requires std::derived_from<std::remove_cvref_t<TDerived>, TMediumDescriptor>
TIntrusivePtr<const TDerived> TMediumDescriptor::As() const
{
    return static_cast<const TDerived*>(this);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
