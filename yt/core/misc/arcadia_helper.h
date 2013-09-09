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
T* operator~(const ::std::unique_ptr<T>& ptr)
{
    return ptr.get();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT