#pragma once

#include "public.h"

#include <ytlib/ytree/public.h>

namespace NYT {
namespace NProfiling {

////////////////////////////////////////////////////////////////////////////////

struct TSample
{
    TSample(i64 id, i64 time, TValue value);

    i64 Id;
    i64 Time;
    TValue Value;
};

////////////////////////////////////////////////////////////////////////////////

class TProfilingManager
{
public:
    TProfilingManager();

    static TProfilingManager* Get();

    void AddSample(const TSample& sample);

private:

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NProfiling
} // namespace NYT
