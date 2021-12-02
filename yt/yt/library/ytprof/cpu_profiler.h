#pragma once

#include <thread>
#include <array>
#include <variant>

#if defined(_linux_)
#include <sys/types.h>
#endif

#include <yt/yt/library/ytprof/profile.pb.h>
#include <yt/yt/library/ytprof/api/api.h>

#include <library/cpp/yt/memory/intrusive_ptr.h>

#include <util/generic/hash.h>
#include <util/datetime/base.h>

#include "queue.h"
#include "mem_reader.h"

namespace NYT::NYTProf {

////////////////////////////////////////////////////////////////////////////////

struct TCpuSample
{
    size_t Tid = 0;
    TString ThreadName;
    std::vector<std::pair<TString, std::variant<TString, i64>>> Tags;
    std::vector<ui64> Backtrace;

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
    int RingBufferLogSize = 20; // 1 MiB
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
    TMemReader Mem_;

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
