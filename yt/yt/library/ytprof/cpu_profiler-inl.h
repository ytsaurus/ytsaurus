#pragma once
#ifndef CPU_PROFILER_INL_H_
#error "Direct inclusion of this file is not allowed, include cpu_profiler.h"
// For the sake of sane code completion.
#include "cpu_profiler.h"
#endif
#undef CPU_PROFILER_INL_H_

namespace NYT::NYTProf {

////////////////////////////////////////////////////////////////////////////////

Y_FORCE_INLINE TCpuProfilerTagGuard::TCpuProfilerTagGuard(TCpuProfilerTag tag)
{
    for (int i = 0; i < MaxActiveTags; i++) {
        if (CpuProfilerTags[i] == 0) {
            CpuProfilerTags[i] = tag;
            TagIndex_ = i;
            return;
        }
    }
}

Y_FORCE_INLINE TCpuProfilerTagGuard::~TCpuProfilerTagGuard()
{
    if (TagIndex_ != -1) {
        CpuProfilerTags[TagIndex_] = 0;
    }
}

Y_FORCE_INLINE TCpuProfilerTagGuard::TCpuProfilerTagGuard(TCpuProfilerTagGuard&& other)
    : TagIndex_(other.TagIndex_)
{
    other.TagIndex_ = -1;
}

Y_FORCE_INLINE TCpuProfilerTagGuard& TCpuProfilerTagGuard::operator = (TCpuProfilerTagGuard&& other)
{
    if (TagIndex_ != -1) {
        CpuProfilerTags[TagIndex_] = 0;
    }

    other.TagIndex_ = -1;
    return *this;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTProf
