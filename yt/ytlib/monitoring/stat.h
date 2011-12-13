#pragma once

#include "statlog.h"
#include "stat-inl.h"

namespace NSTAT {

////////////////////////////////////////////////////////////////////////////////
//
// Scalar measure points

// Single data point
#define DATA_POINT(name, spec, value) NSTAT::LOG_VALUE(name, spec, value)

// Code block time
#define TIMEIT(name, spec, code) { \
    i64 __TIMEIT_time = GetCycleCount(); \
    code; \
    NSTAT::LOG_VALUE(name, spec, GetCycleCount() - __TIMEIT_time); \
}

////////////////////////////////////////////////////////////////////////////////
//
// Vector measure points (or values with living state)
//

// state lives statically in the current frame
// (good for function timing/profiling)
#define FRAME_TIME_START(name) \
    static NSTAT::TTimeCounter _FRAME_TIME_counter_##__LINE__; \
    NSTAT::_StartSeq(&_FRAME_TIME_counter_##__LINE__, name)

#define FRAME_TIME_POINT(name, pointName) \
    NSTAT::_VisitPoint(&_FRAME_TIME_counter_##__LINE__, pointName)

#define FRAME_TIME_END(name) \
    NSTAT::_FinishSeq(&_FRAME_TIME_counter_##__LINE__, name)

// state lives in the TLS, across any frames but local to the current thread
#define THREAD_TIME_START(name) \
    NSTAT::_ThreadTimeStart(name)

#define THREAD_TIME_POINT(name, pointName) \
    NSTAT::_ThreadTimePoint(name, pointName)

#define THREAD_TIME_END(name) \
    NSTAT::_ThreadTimeEnd(name)

////////////////////////////////////////////////////////////////////////////////

}
