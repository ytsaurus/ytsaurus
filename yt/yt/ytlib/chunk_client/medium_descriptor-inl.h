#ifndef MEDIUM_DESCRIPTOR_INL_H_
#error "Direct inclusion of this file is not allowed, include chunk.h"
// For the sake of sane code completion.
#include "medium_descriptor.h"
#endif

namespace NYT::NChunkClient {

////////////////////////////////////////////////////////////////////////////////

template <class TDerived>
TIntrusivePtr<TDerived> TMediumDescriptor::As()
{
    return dynamic_cast<TDerived*>(this);
}

template <class TDerived>
TIntrusivePtr<const TDerived> TMediumDescriptor::As() const
{
    return dynamic_cast<const TDerived*>(this);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
