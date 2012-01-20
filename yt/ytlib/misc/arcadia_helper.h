#pragma once

// Here resides evilish code.
////////////////////////////////////////////////////////////////////////////////

template<class T>
T* operator~(const NYT::TIntrusivePtr<T>& ptr)
{
    return ptr.Get();
}

template<class T>
T* operator~(const TAutoPtr<T>& ptr)
{
    return ptr.Get();
}

template<class T>
T* operator~(const TSharedPtr<T>& ptr)
{
    return ptr.Get();
}

template<class T>
T* operator~(const THolder<T>& ptr)
{
    return ptr.Get();
}
