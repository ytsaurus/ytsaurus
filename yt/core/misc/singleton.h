#pragma once

#include "common.h"

#include <atomic>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class T>
TIntrusivePtr<T> RefCountedSingleton();

#define DECLARE_IMMORTAL_SINGLETON_FRIEND() \
	template <class T>                      \
    friend T* ::NYT::ImmortalSingleton();

template <class T>
T* ImmortalSingleton();

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define SINGLETON_INL_H_
#include "singleton-inl.h"
#undef SINGLETON_INL_H_
