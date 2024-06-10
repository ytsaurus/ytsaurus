#pragma once

#include <bigrt/lib/consuming_system/interface/interface.h>

#include <bigrt/lib/utility/profiling/metric.h>

#include <util/generic/ptr.h>
#include <util/system/spinlock.h>

namespace NRoren {

    struct TVCpuMetrics {
        TSpinLock Lock;
        mutable NBigRT::TMetric<TDuration> Process;
        mutable NBigRT::TMetric<TDuration> PrepareForAsyncWrite;

        void AddPrepareForAsyncWrite(const TDuration& value)
        {
            Add(PrepareForAsyncWrite, value);
        }

        void AddProcess(const TDuration& value)
        {
            Add(Process, value);
        }

        NBigRT::TConsumingSystem::TVCpu Get() const
        {
            with_lock(Lock) {
                NBigRT::TConsumingSystem::TVCpu result = {
                    .Process = Process.Get(),
                    .PrepareForAsyncWrite = PrepareForAsyncWrite.Get(),
                };
                auto total = (result.PrepareForAsyncWrite + result.Process).SecondsFloat() / Process.GetInterval().SecondsFloat();
                result.Total = TDuration::Seconds(total);
                return result;
            }
        }

    private:
        void Add(NBigRT::TMetric<TDuration>& metric, const TDuration& value)
        {
            with_lock(Lock) {
                metric.Push(TInstant::Now(), value);
            }
        }
    };

    using TVCpuMetricsPtr = TAtomicSharedPtr<TVCpuMetrics>;
}
