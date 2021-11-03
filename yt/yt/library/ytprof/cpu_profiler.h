#pragma once

#include <thread>
#include <array>
#include <variant>

#if defined(_linux_)
#include <sys/types.h>
#endif

#include <yt/yt/library/ytprof/profile.pb.h>

#include <yt/yt/library/memory/intrusive_ptr.h>

#include <util/generic/hash.h>
#include <util/datetime/base.h>

#include "queue.h"
#include "mem_reader.h"
#include "atomic_signal_ptr.h"

namespace NYT::NYTProf {

////////////////////////////////////////////////////////////////////////////////

struct TProfilerTag final
{
    TString Name;
    std::optional<TString> StringValue;
    std::optional<i64> IntValue;

    TProfilerTag(const TString& name, const TString& value)
        : Name(name)
        , StringValue(value)
    { }

    TProfilerTag(const TString& name, i64 value)
        : Name(name)
        , IntValue(value)
    { }
};

typedef TIntrusivePtr<TProfilerTag> TProfilerTagPtr;

////////////////////////////////////////////////////////////////////////////////

// Hooks for yt/yt/core fibers.
void* AcquireFiberTagStorage();
std::vector<std::pair<TString, std::variant<TString, i64>>> ReadFiberTags(void* storage);
void ReleaseFiberTagStorage(void* storage);

////////////////////////////////////////////////////////////////////////////////

const int MaxActiveTags = 4;

class TCpuProfilerTagGuard
{
public:
    explicit TCpuProfilerTagGuard(TProfilerTagPtr tag);
    ~TCpuProfilerTagGuard();

    TCpuProfilerTagGuard(TCpuProfilerTagGuard&& other);
    TCpuProfilerTagGuard(const TCpuProfilerTagGuard& other) = delete;

    TCpuProfilerTagGuard& operator = (TCpuProfilerTagGuard&& other);
    TCpuProfilerTagGuard& operator = (const TCpuProfilerTagGuard& other) = delete;

private:
    int TagIndex_ = -1;
};

////////////////////////////////////////////////////////////////////////////////

struct TCpuSample
{
    size_t Tid = 0;
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
