#pragma once

#include <thread>
#include <array>

#if defined(_linux_)
#include <sys/types.h>
#endif

#include <yt/yt/library/ytprof/profile.pb.h>

#include <util/generic/hash.h>
#include <util/datetime/base.h>

#include "queue.h"

namespace NYT::NYTProf {

////////////////////////////////////////////////////////////////////////////////

typedef uint64_t TCpuProfilerTag;

// NB: Tag allocation leaks memory by design.
TCpuProfilerTag NewStringTag(const TString& name, const TString& value);
TCpuProfilerTag NewIntTag(const TString& name, const ui64 value);

////////////////////////////////////////////////////////////////////////////////

const int MaxActiveTags = 4;

// Do not access this field directly. It is exposed here for YT fiber scheduler.
extern thread_local std::array<volatile TCpuProfilerTag, MaxActiveTags> CpuProfilerTags;

class TCpuProfilerTagGuard
{
public:
    explicit TCpuProfilerTagGuard(TCpuProfilerTag tag);
    ~TCpuProfilerTagGuard();

    TCpuProfilerTagGuard(TCpuProfilerTagGuard&& other);
    TCpuProfilerTagGuard(const TCpuProfilerTagGuard& other) = delete;

    TCpuProfilerTagGuard& operator = (TCpuProfilerTagGuard&& other);
    TCpuProfilerTagGuard& operator = (const TCpuProfilerTagGuard& other) = delete;

private:
    int TagIndex_ = -1;
};

////////////////////////////////////////////////////////////////////////////////

struct TCpuTagImpl;

////////////////////////////////////////////////////////////////////////////////

struct TCpuSample
{
    size_t Tid = 0;
    std::vector<TCpuTagImpl*> Tags;
    std::vector<std::pair<void*, void*>> Backtrace;

    bool operator == (const TCpuSample& other) const = default;
    operator size_t() const;
};

////////////////////////////////////////////////////////////////////////////////

class TCpuProfilerOptions
{
public:
    int SamplingFrequency = 100;
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
    NProto::Profile ReadProfile();

private:
#if defined(_linux_)
    const TCpuProfilerOptions Options_;

    static std::atomic<TCpuProfiler*> ActiveProfiler_;
    static std::atomic<bool> HandlingSigprof_;
    static void SigProfHandler(int sig, siginfo_t* info, void* ucontext);

    std::atomic<bool> Stop_{true};
    std::atomic<i64> QueueOverflows_{0};
    std::atomic<i64> SignalOverruns_{0};

    std::pair<void*, void*> VdsoRange_;

    TStaticQueue Queue_;
    std::thread BackgroundThread_;

    THashMap<TCpuSample, int> Counters_;

    void StartTimer();
    void StopTimer();
    void DequeueSamples();

    void OnSigProf(siginfo_t* info, ucontext_t* ucontext);
#endif
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTProf

#define CPU_PROFILER_INL_H_
#include "cpu_profiler-inl.h"
#undef CPU_PROFILER_INL_H_
