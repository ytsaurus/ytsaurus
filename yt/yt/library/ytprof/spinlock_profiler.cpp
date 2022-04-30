#include "spinlock_profiler.h"

#include <absl/base/internal/spinlock.h>
#include <absl/base/internal/cycleclock.h>

#include <util/system/yield.h>

namespace NYT::NYTProf {

////////////////////////////////////////////////////////////////////////////////

TSpinlockProfiler::TSpinlockProfiler(TSpinlockProfilerOptions options)
    : TSignalSafeProfiler(options)
    , Options_(options)
{ }

TSpinlockProfiler::~TSpinlockProfiler()
{
    Stop();
}

std::atomic<int> TSpinlockProfiler::SamplingRate_{0};
std::atomic<TSpinlockProfiler*> TSpinlockProfiler::ActiveProfiler_{nullptr};
std::atomic<bool> TSpinlockProfiler::HandlingEvent_{false};

void TSpinlockProfiler::EnableProfiler()
{
    static bool hookInitialized = [] {
        absl::base_internal::RegisterSpinLockProfiler(&TSpinlockProfiler::OnEvent);
        return true;
    }();
    Y_UNUSED(hookInitialized);

    TSpinlockProfiler* expected = nullptr;
    if (!ActiveProfiler_.compare_exchange_strong(expected, this)) {
        throw yexception() << "another instance of spinlock profiler is running";
    }
    SamplingRate_ = Options_.ProfileFraction;
}

void TSpinlockProfiler::DisableProfiler()
{
    SamplingRate_ = 0;
    ActiveProfiler_ = nullptr;
    while (HandlingEvent_) {
        SchedYield();
    }
}

void TSpinlockProfiler::RecordEvent(const void *lock, int64_t waitCycles)
{
    Y_UNUSED(lock);

    unw_context_t context;
    YT_VERIFY(unw_getcontext(&context) == 0);

    unw_cursor_t cursor;
    YT_VERIFY(unw_init_local(&cursor, &context) == 0);

    unw_word_t ip = 0;
    YT_VERIFY(unw_get_reg(&cursor, UNW_REG_IP, &ip) == 0);

    unw_word_t rsp = 0;
    YT_VERIFY(unw_get_reg(&cursor, UNW_X86_64_RSP, &rsp) == 0);

    unw_word_t rbp = 0;
    YT_VERIFY(unw_get_reg(&cursor, UNW_X86_64_RBP, &rbp) == 0);

    TFramePointerCursor fpCursor(
        &Mem_,
        reinterpret_cast<void*>(ip),
        reinterpret_cast<void*>(rsp),
        reinterpret_cast<void*>(rbp));

    RecordSample(&fpCursor, waitCycles);
}

static thread_local int SpinlockEventCount = 0;

void TSpinlockProfiler::OnEvent(const void *lock, int64_t waitCycles)
{
    auto samplingRate = SamplingRate_.load(std::memory_order_relaxed);
    if (samplingRate == 0) {
        return;
    }

    if (SpinlockEventCount < samplingRate) {
        SpinlockEventCount++;
        return;
    }

    SpinlockEventCount = 0;
    while (HandlingEvent_.exchange(true)) {
        SchedYield();
    }

    auto profiler = ActiveProfiler_.load();
    if (profiler) {
        profiler->RecordEvent(lock, waitCycles);
    }

    HandlingEvent_.store(false);
}

void TSpinlockProfiler::AnnotateProfile(NProto::Profile* profile, std::function<i64(const TString&)> stringify)
{
    auto sampleType = profile->add_sample_type();
    sampleType->set_type(stringify("sample"));
    sampleType->set_unit(stringify("count"));

    sampleType = profile->add_sample_type();
    sampleType->set_type(stringify("cpu"));
    sampleType->set_unit(stringify("nanoseconds"));

    auto periodType = profile->mutable_period_type();
    periodType->set_type(stringify("sample"));
    periodType->set_unit(stringify("count"));

    profile->set_period(Options_.ProfileFraction);
}

i64 TSpinlockProfiler::TransformValue(i64 value)
{
    return value / absl::base_internal::CycleClock::Frequency() * 1e9;
}

////////////////////////////////////////////////////////////////////////////////


TBlockingProfiler::TBlockingProfiler(TSpinlockProfilerOptions options)
    : TSignalSafeProfiler(options)
    , Options_(options)
{ }

TBlockingProfiler::~TBlockingProfiler()
{
    Stop();
}

std::atomic<int> TBlockingProfiler::SamplingRate_{0};
std::atomic<TBlockingProfiler*> TBlockingProfiler::ActiveProfiler_{nullptr};
std::atomic<bool> TBlockingProfiler::HandlingEvent_{false};

void TBlockingProfiler::EnableProfiler()
{
    static bool hookInitialized = [] {
        NThreading::RegisterSpinWaitSlowPathHook(&TBlockingProfiler::OnEvent);
        return true;
    }();
    Y_UNUSED(hookInitialized);

    TBlockingProfiler* expected = nullptr;
    if (!ActiveProfiler_.compare_exchange_strong(expected, this)) {
        throw yexception() << "another instance of spinlock profiler is running";
    }
    SamplingRate_ = Options_.ProfileFraction;
}

void TBlockingProfiler::DisableProfiler()
{
    SamplingRate_ = 0;
    ActiveProfiler_ = nullptr;
    while (HandlingEvent_) {
        SchedYield();
    }
}

void TBlockingProfiler::RecordEvent(
    TCpuDuration cpuDelay,
    const ::TSourceLocation& location,
    NThreading::ESpinLockActivityKind activityKind)
{
    Y_UNUSED(location, activityKind);

    unw_context_t context;
    YT_VERIFY(unw_getcontext(&context) == 0);

    unw_cursor_t cursor;
    YT_VERIFY(unw_init_local(&cursor, &context) == 0);

    unw_word_t ip = 0;
    YT_VERIFY(unw_get_reg(&cursor, UNW_REG_IP, &ip) == 0);

    unw_word_t rsp = 0;
    YT_VERIFY(unw_get_reg(&cursor, UNW_X86_64_RSP, &rsp) == 0);

    unw_word_t rbp = 0;
    YT_VERIFY(unw_get_reg(&cursor, UNW_X86_64_RBP, &rbp) == 0);

    TFramePointerCursor fpCursor(
        &Mem_,
        reinterpret_cast<void*>(ip),
        reinterpret_cast<void*>(rsp),
        reinterpret_cast<void*>(rbp));

    RecordSample(&fpCursor, cpuDelay);
}

static thread_local int YTSpinlockEventCount = 0;

void TBlockingProfiler::OnEvent(
    TCpuDuration cpuDelay,
    const ::TSourceLocation& location,
    NThreading::ESpinLockActivityKind activityKind)
{
    auto samplingRate = SamplingRate_.load(std::memory_order_relaxed);
    if (samplingRate == 0) {
        return;
    }

    if (YTSpinlockEventCount < samplingRate) {
        YTSpinlockEventCount++;
        return;
    }

    YTSpinlockEventCount = 0;
    while (HandlingEvent_.exchange(true)) {
        SchedYield();
    }

    auto profiler = ActiveProfiler_.load();
    if (profiler) {
        profiler->RecordEvent(cpuDelay, location, activityKind);
    }

    HandlingEvent_.store(false);
}

void TBlockingProfiler::AnnotateProfile(NProto::Profile* profile, std::function<i64(const TString&)> stringify)
{
    auto sampleType = profile->add_sample_type();
    sampleType->set_type(stringify("sample"));
    sampleType->set_unit(stringify("count"));

    sampleType = profile->add_sample_type();
    sampleType->set_type(stringify("cpu"));
    sampleType->set_unit(stringify("nanoseconds"));

    auto periodType = profile->mutable_period_type();
    periodType->set_type(stringify("sample"));
    periodType->set_unit(stringify("count"));

    profile->set_period(Options_.ProfileFraction);
}

i64 TBlockingProfiler::TransformValue(i64 value)
{
    return CpuDurationToDuration(value).NanoSeconds();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTProf
