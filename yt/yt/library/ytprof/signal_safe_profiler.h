#pragma once

#include <thread>
#include <array>
#include <variant>

#include <yt/yt/core/misc/safe_memory_reader.h>

#include <yt/yt/library/ytprof/profile.pb.h>
#include <yt/yt/library/ytprof/api/api.h>

#include <library/cpp/yt/memory/intrusive_ptr.h>

#include <util/generic/hash.h>
#include <util/datetime/base.h>

#include "queue.h"
#include "backtrace.h"

namespace NYT::NYTProf {

////////////////////////////////////////////////////////////////////////////////

struct TProfileLocation
{
    size_t Tid = 0;
    TString ThreadName;
    std::vector<std::pair<TString, std::variant<TString, i64>>> Tags;
    std::vector<ui64> Backtrace;

    bool operator == (const TProfileLocation& other) const = default;
    operator size_t() const;
};

////////////////////////////////////////////////////////////////////////////////

class TSignalSafeProfilerOptions
{
public:
    TDuration DequeuePeriod = TDuration::MilliSeconds(100);
    int MaxBacktraceSize = 256;
    int RingBufferLogSize = 20; // 1 MiB
    bool RecordActionRunTime = false;
};

////////////////////////////////////////////////////////////////////////////////

class TSignalSafeProfiler
{
public:
    TSignalSafeProfiler(TSignalSafeProfilerOptions options = {});
    virtual ~TSignalSafeProfiler();

    void Start();
    void Stop();

    //! ReadProfile returns accumulated profile.
    /*!
     *  This function may be called only after profiler is stopped.
     */
    NProto::Profile ReadProfile();

protected:
    const TSignalSafeProfilerOptions Options_;
    TSafeMemoryReader Mem_;

    std::atomic<bool> Stop_{true};
    std::atomic<i64> QueueOverflows_{0};

    TStaticQueue Queue_;
    std::thread BackgroundThread_;

    struct TProfileCounter
    {
        i64 Count = 0;
        i64 Total = 0;
    };

    THashMap<TProfileLocation, TProfileCounter> Counters_;

    virtual void EnableProfiler() = 0;
    virtual void DisableProfiler() = 0;
    virtual void AnnotateProfile(NProto::Profile* profile, const std::function<i64(const TString&)>& stringify) = 0;
    virtual i64 EncodeValue(i64 value) = 0;

    void RecordSample(TFramePointerCursor* cursor, i64 value);

    void DequeueSamples();
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTProf
