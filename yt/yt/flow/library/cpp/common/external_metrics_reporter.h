#pragma once

#include "public.h"

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

//! Interface for reporting external job metrics such as CPU time and memory usage.
//! This is used to track resource consumption of computations running
//! outside the main Flow framework, e.g. in companion processes.
struct IExternalPerformanceMetricsReporter
    : public TRefCounted
{
    //! Sets the current memory usage value.
    /*!
     *  \param memoryUsage The memory usage in bytes.
     */
    virtual void SetMemoryUsage(size_t memoryUsage) = 0;

    //! Adds the given CPU time to the total elapsed CPU time.
    /*!
     *  \param cpuTime The CPU time to add.
     */
    virtual void AddCpuTime(TDuration cpuTime) = 0;
};

DEFINE_REFCOUNTED_TYPE(IExternalPerformanceMetricsReporter);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
