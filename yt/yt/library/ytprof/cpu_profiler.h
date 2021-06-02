#pragma once

#include <thread>
#include <array>

#include <yt/yt/library/ytprof/profile.pb.h>

#include <util/generic/hash.h>
#include <util/datetime/base.h>

#include "queue.h"

namespace NYT::NProf {

////////////////////////////////////////////////////////////////////////////////

struct TCpuSample
{
    std::vector<std::pair<void*, void*>> Backtrace;

    bool operator == (const TCpuSample& other) const = default;
    operator size_t() const;
};

////////////////////////////////////////////////////////////////////////////////

class TCpuProfilerOptions
{
public:
    int SamplingFrequency = 1;
    TDuration DequeuePeriod = TDuration::MilliSeconds(100);
    int MaxBacktraceSize = 256;
    int RingBufferLogSize = 16; // 32KiB
};

////////////////////////////////////////////////////////////////////////////////

class TCpuProfiler
{
public:
    TCpuProfiler(TCpuProfilerOptions options = {});
    ~TCpuProfiler();

    void Start();
    void Stop();

    //! ReadProfile returns accumulated profile.
    /*!
     *  This function may be called only after profiler is stopped.
     */
    Profile ReadProfile();

private:
    const TCpuProfilerOptions Options_;

    static std::atomic<TCpuProfiler*> ActiveProfiler_;
    static std::atomic<bool> HandlingSigprof_;
    static void SigProfHandler(int sig, siginfo_t* info, void* ucontext);

    std::atomic<bool> Stop_{false};
    std::atomic<i64> QueueOverflows_{0};
    std::atomic<i64> SignalOverruns_{0};

    std::pair<void*, void*> VDSORange_;

    TStaticQueue Queue_;
    std::thread BackgroundThread_;

    THashMap<TCpuSample, int> Counters_;

    void StartTimer();
    void StopTimer();
    void DequeueSamples();

    void OnSigProf(siginfo_t* info, ucontext_t* ucontext);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NProf
