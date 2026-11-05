#pragma once

#ifndef RESOURCE_INL_H_
    #error "Direct inclusion of this file is not allowed, include resource.h"
    // For the sake of sane code completion.
    #include "resource.h"
#endif

#include <yt/yt/core/misc/error.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

template <class TDerivedResource>
TIntrusivePtr<TDerivedResource> IResource::As()
{
    auto derived = MakeStrong(dynamic_cast<TDerivedResource*>(this));
    THROW_ERROR_EXCEPTION_IF(!derived, "Failed to cast resource to %v", TypeName<TDerivedResource>());
    return derived;
}

template <class TDerivedResource>
TIntrusivePtr<TDerivedResource> IResource::TryAs()
{
    return MakeStrong(dynamic_cast<TDerivedResource*>(this));
}

template <class TDerivedResource>
TIntrusivePtr<const TDerivedResource> IResource::As() const
{
    auto derived = MakeStrong(dynamic_cast<const TDerivedResource*>(this));
    THROW_ERROR_EXCEPTION_IF(!derived, "Failed to cast resource to %v", TypeName<TDerivedResource>());
    return derived;
}

template <class TDerivedResource>
TIntrusivePtr<const TDerivedResource> IResource::TryAs() const
{
    return MakeStrong(dynamic_cast<const TDerivedResource*>(this));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
