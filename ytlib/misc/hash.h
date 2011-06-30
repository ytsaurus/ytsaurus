#pragma once

#include <util/str_stl.h>

namespace NYT {

template <class T>
struct TIntrusivePtrHash
{
    size_t operator () (const TIntrusivePtr<T>& ptr) const
    {
        return THash<T*>()(~ptr);
    }
};

} // namespace NYT
