#pragma once

#include "intrusive_ptr.h"

#include <util/str_stl.h>

////////////////////////////////////////////////////////////////////////////////

//! A hasher for TIntrusivePtr.
template <class T>
struct hash< NYT::TIntrusivePtr<T> >
{
    size_t operator () (const NYT::TIntrusivePtr<T>& ptr) const
    {
        return THash<T*>()(~ptr);
    }
};

////////////////////////////////////////////////////////////////////////////////
