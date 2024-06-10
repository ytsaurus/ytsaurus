#pragma once

#include <yt/cpp/roren/interface/roren.h>

namespace NRoren {

////////////////////////////////////////////////////////////////////////////////

template <typename T>
NRoren::TTransform<T, T> ReportThrottlerProcessed(ui64 (*sizeFn)(const T&), ui64 flushSize = 256);

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren

#define THROTTLE_INL_H_
#include "throttle-inl.h"
#undef THROTTLE_INL_H_
