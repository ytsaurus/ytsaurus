#pragma once

#include "signal_safe_profiler.h"

namespace NYT::NYTProf {

////////////////////////////////////////////////////////////////////////////////

struct TSpinlockProfilerOptions
    : public TSignalSafeProfilerOptions
{
    int ProfileFraction = 100;
};

////////////////////////////////////////////////////////////////////////////////

class TSpinlockProfiler
    : public TSignalSafeProfiler
{
public:
    TSpinlockProfiler(TSpinlockProfilerOptions options);
    ~TSpinlockProfiler();

private:
    const TSpinlockProfilerOptions Options_;

    static std::atomic<int> SamplingRate_;
    static std::atomic<TSpinlockProfiler*> ActiveProfiler_;
    static std::atomic<bool> HandlingEvent_;

    void EnableProfiler() override;
    void DisableProfiler() override;
    void AnnotateProfile(NProto::Profile* profile, std::function<i64(const TString&)> stringify) override;

    static void OnEvent(const void *lock, int64_t waitCycles);
    void RecordEvent(const void *lock, int64_t waitCycles);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTProf
