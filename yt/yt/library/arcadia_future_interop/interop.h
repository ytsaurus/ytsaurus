#pragma once

#include <yt/yt/core/actions/future.h>

#include <library/cpp/threading/future/core/future.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class T>
::NThreading::TFuture<T> ToArcadiaFuture(const TFuture<T>& future);

template <class T>
TFuture<T> FromArcadiaFuture(const ::NThreading::TFuture<T>& future);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define INTEROP_INL_H_
#include "interop-inl.h"
#undef INTEROP_INL_H_
