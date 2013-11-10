#pragma once

#include "intrusive_ptr.h"

namespace std {

////////////////////////////////////////////////////////////////////////////////

//! A hasher for TIntrusivePtr.
template <class T>
struct hash<NYT::TIntrusivePtr<T>>
{
    size_t operator () (const NYT::TIntrusivePtr<T>& ptr) const
    {
        return THash<T*>()(~ptr);
    }
};

//! A hasher for std::pair.
template <class T1, class T2>
struct hash<std::pair<T1, T2>>
{
    size_t operator () (const std::pair<T1, T2>& pair) const
    {
        return THash<T1>()(pair.first) + 1877 * THash<T2>()(pair.second);
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace std
