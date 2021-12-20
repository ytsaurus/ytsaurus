#pragma once

#include "public.h"

#include <library/cpp/yt/memory/intrusive_ptr.h>

namespace NYT::NDI {

////////////////////////////////////////////////////////////////////////////////

template<class T, auto Kind>
class TTaggedPtr
    : public TIntrusivePtr<T>
{
public:
    using EEnum = decltype(Kind);
    static constexpr auto ValueKind = Kind;

    TTaggedPtr() = default;

    explicit TTaggedPtr(TIntrusivePtr<T> ptr)
        : TIntrusivePtr<T>(std::move(ptr))
    { }
};

////////////////////////////////////////////////////////////////////////////////

}
