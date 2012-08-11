#pragma once

// Here resides evilish code.

namespace NYT
{

////////////////////////////////////////////////////////////////////////////////

template <class T>
T* operator~(const TIntrusivePtr<T>& ptr)
{
    return ptr.Get();
}

template <class T>
T* operator~(const ::TAutoPtr<T>& ptr)
{
    return ptr.Get();
}

template <class T>
T* operator~(const ::THolder<T>& ptr)
{
    return ptr.Get();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT