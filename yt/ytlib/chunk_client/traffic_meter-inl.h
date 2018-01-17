#pragma once
#ifndef TRAFFIC_METER_INL_H_
#error "Direct inclusion of this file is not allowed, include traffic_meter.h"
#endif

namespace NYT {
namespace NChunkClient {

////////////////////////////////////////////////////////////////////////////////

template <typename T>
void TTrafficMeter::IncrementByteCountImpl(yhash<T, i64>& data, const T& key, i64 byteCount)
{
    TGuard<TSpinLock> guard(Lock_);
    data[key] += byteCount;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
