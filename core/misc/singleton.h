#pragma once

#include "common.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

#define DECLARE_LEAKY_SINGLETON_FRIEND() \
    template <class T>                   \
    friend T* ::NYT::LeakySingleton();

template <class T>
T* LeakySingleton();

template <class T>
TIntrusivePtr<T> RefCountedSingleton();

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define SINGLETON_INL_H_
#include "singleton-inl.h"
#undef SINGLETON_INL_H_
