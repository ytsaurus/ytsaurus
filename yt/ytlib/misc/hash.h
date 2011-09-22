#pragma once

#include "ptr.h"

#include <util/str_stl.h>

////////////////////////////////////////////////////////////////////////////////

//! A hasher for TIntrusivePtr.
template <class T>
struct hash< TIntrusivePtr<T> >
{
    size_t operator () (const TIntrusivePtr<T>& ptr) const
    {
        return THash<T*>()(~ptr);
    }
};

//! A hasher for TIntrusiveConstPtr.
template <class T>
struct hash< TIntrusiveConstPtr<T> >
{
    size_t operator () (const TIntrusiveConstPtr<T>& ptr) const
    {
        return THash<T*>()(~ptr);
    }
};

////////////////////////////////////////////////////////////////////////////////

