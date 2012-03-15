#include "stdafx.h"
#include "profiling_manager.h"

#include <ytlib/misc/id_generator.h>
#include <ytlib/actions/action_queue.h>
#include <ytlib/ytree/ytree.h>
#include <ytlib/ytree/ephemeral.h>
#include <ytlib/ytree/ypath_detail.h>
#include <ytlib/ytree/virtual.h>
#include <ytlib/ytree/ypath_client.h>
#include <ytlib/ytree/fluent.h>

#include <util/datetime/cputimer.h>

// TODO(babenko): get rid of this dependency on NHPTimer
#include <quality/Misc/HPTimer.h>

namespace NYT {
namespace NProfiling  {


using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger Logger("Profiling");

////////////////////////////////////////////////////////////////////////////////

struct TProfilingManager::TStoredSample
{
    i64 Id;
    TInstant Time;
    TValue Value;
};

////////////////////////////////////////////////////////////////////////////////

class TProfilingManager::TBucket
    : public TYPathServiceBase
    , public TSupportsGet
{
public:
    typedef std::deque<TStoredSample> TSamples;
    typedef TSamples::iterator TSamplesIterator;
    typedef std::pair<TSamplesIterator, TSamplesIterator> TSamplesRange;

    TBucket()
        : TYPathServiceBase("Profiling")
    { }

    //! Adds a new sample to the bucket inserting in at an appropriate position.
    void AddSample(const TStoredSample& sample)
    {
        // Samples are ordered by time.
        // Search for an appropriate insertion point starting from the the back,
        // this should usually be fast.
        int index = static_cast<int>(Samples.size());
        while (index > 0 && Samples[index - 1].Time > sample.Time) {
            --index;
        }
        Samples.insert(Samples.begin() + index, sample);
    }

    //! Removes the oldest samples keeping [minTime,maxTime] interval no larger than #maxKeepInterval.
    void TrimSamples(TDuration maxKeepInterval)
    {
        if (Samples.size() <= 1)
            return;

        auto deadline = Samples.back().Time - maxKeepInterval;
        while (Samples.front().Time < deadline) {
            Samples.pop_front();
        }
    }

    //! Gets samples with timestamp larger than #lastTime.
    //! If #lastTime is #Null then all samples are returned.
    TSamplesRange GetSamples(TNullable<TInstant> lastTime = Null)
    {
        if (!lastTime) {
            return make_pair(Samples.begin(), Samples.end());
        }

        // Run binary search to find the proper position.
        TStoredSample lastSample;
        lastSample.Time = lastTime.Get();
        auto it = std::upper_bound(
            Samples.begin(),
            Samples.end(),
            lastSample,
            [=] (const TStoredSample& lhs, const TStoredSample& rhs) { return lhs.Time < rhs.Time; });

        return std::make_pair(it, Samples.end());
    }

private:
    std::deque<TStoredSample> Samples;

    void DoInvoke(NRpc::IServiceContext* context)
    {
        DISPATCH_YPATH_SERVICE_METHOD(Get);
        TYPathServiceBase::DoInvoke(context);
    }

    static TNullable<TInstant> ParseInstant(TNullable<i64> value)
    {
        return value ? MakeNullable(TInstant::MicroSeconds(value.Get())) : Null;
    }

    void GetSelf(TReqGet* request, TRspGet* response, TCtxGet* context)
    {
        auto fromTime = ParseInstant(request->Attributes().Find<i64>("from_time"));
        auto range = GetSamples(fromTime);
        TYson yson = BuildYsonFluently()
            .DoListFor(range.first, range.second, [] (TFluentList fluent, const TSamplesIterator& it)
                {
                    const auto& sample = *it;
                    fluent
                        .Item().BeginMap()
                            .Item("id").Scalar(sample.Id)
                            .Item("time").Scalar(static_cast<i64>(sample.Time.MicroSeconds()))
                            .Item("value").Scalar(sample.Value)
                        .EndMap();
                });
        response->set_value(yson);
        context->Reply();
    }

};

////////////////////////////////////////////////////////////////////////////////

class TProfilingManager::TClockConverter
{
public:
    TClockConverter()
        : NextCalibrationClock(0)
    { }

    TInstant ToInstant(ui64 clock)
    {
        CalibrateIfNeeded();
        // TDuration is unsigned and thus does not support negative values.
        return
            clock >= CalibrationClock
            ? CalibrationInstant + ClockToDuration(clock - CalibrationClock)
            : CalibrationInstant - ClockToDuration(CalibrationClock - clock);
    }

private:
    static const TDuration CalibrationInterval;

    // TODO(babenko): get rid of this dependency on NHPTimer
    static TDuration ClockToDuration(i64 cycles)
    {
        return TDuration::Seconds((double) cycles / NHPTimer::GetClockRate());
    }

    void CalibrateIfNeeded()
    {
        auto nowClock = GetCycleCount();
        if (nowClock > NextCalibrationClock) {
            Calibrate();
        }
    }

    void Calibrate()
    {
        auto nowClock = GetCycleCount();
        auto nowInstant = TInstant::Now();
        if (NextCalibrationClock != 0) {
            auto expected = (CalibrationInstant + ClockToDuration(nowClock - CalibrationClock)).MicroSeconds();
            auto actual = nowInstant.MicroSeconds();
            LOG_DEBUG("Clock recalibrated (Diff: %" PRId64 ")", expected - actual);
        }
        CalibrationClock = nowClock;
        CalibrationInstant = nowInstant;
        NextCalibrationClock = nowClock + DurationToCycles(CalibrationInterval);
    }

    TInstant CalibrationInstant;
    ui64 CalibrationClock;
    ui64 NextCalibrationClock;

};

const TDuration TProfilingManager::TClockConverter::CalibrationInterval = TDuration::Seconds(3);

////////////////////////////////////////////////////////////////////////////////

class TProfilingManager::TImpl
    : public TActionQueueBase
{
public:
    TImpl()
        : TActionQueueBase("Profiling", true)
        , Invoker(New<TQueueInvoker>("Profiling", this, true))
        , Root(GetEphemeralNodeFactory()->CreateMap())
    { }

    ~TImpl()
    {
        Invoker->Shutdown();
        Shutdown();
    }

    void Start()
    {
        TActionQueueBase::Start();
    }
    
    void Shutdown()
    {
        TActionQueueBase::Shutdown();
    }

    void Enqueue(const TQueuedSample& sample)
    {
        if (!IsRunning())
            return;

        SampleQueue.Enqueue(sample);
        Signal();
    }

    IInvoker* GetInvoker() const
    {
        return ~Invoker; 
    }

    IMapNode* GetRoot() const
    {
        return ~Root;
    }

private:
    TQueueInvokerPtr Invoker;
    IMapNodePtr Root;
    TLockFreeQueue<TQueuedSample> SampleQueue;
    yhash_map<TYPath, TWeakPtr<TBucket> > PathToBucket;
    TIdGenerator<i64> IdGenerator;
    TClockConverter ClockConverter;

    static const TDuration MaxKeepInterval;

    bool DequeueAndExecute()
    {
        // Handle pending callbacks first.
        if (Invoker->DequeueAndExecute()) {
            return true;
        }

        // Process all pending samples in a row.
        bool samplesProcessed = false;
        TQueuedSample sample;
        while (SampleQueue.Dequeue(&sample)) {
            ProcessSample(sample);
            samplesProcessed = true;
        }

        return samplesProcessed;
    }

    TBucketPtr LookupBucket(const TYPath& path)
    {
        auto it = PathToBucket.find(path);
        if (it != PathToBucket.end()) {
            auto bucket = it->second.Lock();
            if (bucket) {
                return bucket;
            }
        }

        LOG_INFO("Creating bucket %s", ~path.Quote());
        auto bucket = New<TBucket>();
        PathToBucket[path] = bucket;

        auto node = CreateVirtualNode(~bucket);
        SyncYPathSetNode(~Root, path, ~node);

        return bucket;
    }

    // TODO(babenko): currently not used
    void SweepBucketCache()
    {
        auto it = PathToBucket.begin();
        while (it != PathToBucket.end()) {
            auto jt = it;
            ++jt;
            auto node = it->second.Lock();
            if (!node) {
                PathToBucket.erase(it);
            }
            it = jt;
        }
    }

    void ProcessSample(TQueuedSample& queuedSample)
    {
        auto bucket = LookupBucket(queuedSample.Path);

        TStoredSample storedSample;
        storedSample.Id = IdGenerator.Next();
        storedSample.Time = ClockConverter.ToInstant(queuedSample.Time);
        storedSample.Value = queuedSample.Value;

        bucket->AddSample(storedSample);
        bucket->TrimSamples(MaxKeepInterval);
    }

};

// TODO(babenko): make configurable?
const TDuration TProfilingManager::TImpl::MaxKeepInterval = TDuration::Seconds(60);

////////////////////////////////////////////////////////////////////////////////

TProfilingManager::TProfilingManager()
    : Impl(New<TImpl>())
{ }

TProfilingManager* TProfilingManager::Get()
{
    return Singleton<TProfilingManager>();
}

void TProfilingManager::Start()
{
    Impl->Start();
}

void TProfilingManager::Shutdown()
{
    Impl->Shutdown();
}

void TProfilingManager::Enqueue(const TQueuedSample& sample)
{
    Impl->Enqueue(sample);
}

IInvoker* TProfilingManager::GetInvoker() const
{
    return Impl->GetInvoker();
}

NYTree::IMapNode* TProfilingManager::GetRoot() const
{
    return Impl->GetRoot();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NProfiling
} // namespace NYT
