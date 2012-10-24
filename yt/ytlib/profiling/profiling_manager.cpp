#include "stdafx.h"
#include "profiling_manager.h"
#include "resource_tracker.h"
#include "timing.h"

#include <ytlib/misc/id_generator.h>
#include <ytlib/misc/periodic_invoker.h>

#include <ytlib/actions/action_queue_detail.h>

#include <ytlib/ytree/node.h>
#include <ytlib/ytree/ephemeral_node_factory.h>
#include <ytlib/ytree/ypath_detail.h>
#include <ytlib/ytree/virtual.h>
#include <ytlib/ytree/ypath_client.h>
#include <ytlib/ytree/fluent.h>

#include <ytlib/logging/log.h>

namespace NYT {
namespace NProfiling  {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger Logger("Profiling");
static TProfiler Profiler("/profiling", true);

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
    {
        Logger = NProfiling::Logger;
    }

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

    virtual void DoInvoke(NRpc::IServiceContextPtr context) override
    {
        DISPATCH_YPATH_SERVICE_METHOD(Get);
        TYPathServiceBase::DoInvoke(context);
    }

    static TNullable<TInstant> ParseInstant(TNullable<i64> value)
    {
        return value ? MakeNullable(TInstant::MicroSeconds(value.Get())) : Null;
    }

    virtual void GetSelf(TReqGet* request, TRspGet* response, TCtxGetPtr context) override
    {
        context->SetRequestInfo("");
        auto fromTime = ParseInstant(request->Attributes().Find<i64>("from_time"));
        auto range = GetSamples(fromTime);
        response->set_value(BuildYsonStringFluently()
            .DoListFor(range.first, range.second, [] (TFluentList fluent, const TSamplesIterator& it) {
                const auto& sample = *it;
                fluent
                    .Item().BeginMap()
                        .Item("id").Scalar(sample.Id)
                        .Item("time").Scalar(static_cast<i64>(sample.Time.MicroSeconds()))
                        .Item("value").Scalar(sample.Value)
                    .EndMap();
            }).Data());
        context->Reply();
    }

};

////////////////////////////////////////////////////////////////////////////////

class TProfilingManager::TImpl
    : public TActionQueueBase
{
public:
    TImpl()
        : TActionQueueBase("Profiling", true)
        , Invoker(New<TQueueInvoker>("/Profiling", this, true))
        , Root(GetEphemeralNodeFactory()->CreateMap())
        , EnqueueCounter("/enqueue_rate")
        , DequeueCounter("/dequeue_rate")
    {
#ifndef _win_
        ResourceTracker = New<TResourceTracker>(GetInvoker());
#endif
    }

    ~TImpl()
    {
        Invoker->Shutdown();
        Shutdown();
    }

    void Start()
    {
        TActionQueueBase::Start();
#ifndef _win_
        ResourceTracker->Start();
#endif
    }
    
    void Shutdown()
    {
        TActionQueueBase::Shutdown();
    }

    void Enqueue(const TQueuedSample& sample, bool selfProfiling)
    {
        if (!IsRunning())
            return;

        if (!selfProfiling) {
            Profiler.Increment(EnqueueCounter);
        }

        SampleQueue.Enqueue(sample);
        Signal();
    }

    IInvokerPtr GetInvoker() const
    {
        return Invoker; 
    }

    IMapNodePtr GetRoot() const
    {
        return Root;
    }

private:
    TQueueInvokerPtr Invoker;
    IMapNodePtr Root;
    TRateCounter EnqueueCounter;
    TRateCounter DequeueCounter;

    TLockFreeQueue<TQueuedSample> SampleQueue;
    yhash_map<TYPath, TWeakPtr<TBucket> > PathToBucket;
    TIdGenerator<i64> IdGenerator;

#ifndef _win_
    TIntrusivePtr<TResourceTracker> ResourceTracker;
#endif

    static const TDuration MaxKeepInterval;

    bool DequeueAndExecute()
    {
        // Handle pending callbacks first.
        if (Invoker->DequeueAndExecute()) {
            return true;
        }

        // Process all pending samples in a row.
        int samplesProcessed = 0;
        TQueuedSample sample;
        while (SampleQueue.Dequeue(&sample)) {
            ProcessSample(sample);
            samplesProcessed = true;
        }

        Profiler.Increment(DequeueCounter, samplesProcessed);

        return samplesProcessed > 0;
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

        LOG_DEBUG("Creating bucket %s", ~path);
        auto bucket = New<TBucket>();
        YCHECK(PathToBucket.insert(MakePair(path, bucket)).second);

        auto node = CreateVirtualNode(bucket);
        ForceYPath(Root, path);
        SetNodeByYPath(Root, path, node);

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
        storedSample.Time = CpuInstantToInstant(queuedSample.Time);
        storedSample.Value = queuedSample.Value;

        bucket->AddSample(storedSample);
        bucket->TrimSamples(MaxKeepInterval);
    }
};

// TODO(babenko): make yson serializable
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

void TProfilingManager::Enqueue(const TQueuedSample& sample, bool selfProfiling)
{
    Impl->Enqueue(sample, selfProfiling);
}

IInvokerPtr TProfilingManager::GetInvoker() const
{
    return Impl->GetInvoker();
}

NYTree::IMapNodePtr TProfilingManager::GetRoot() const
{
    return Impl->GetRoot();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NProfiling
} // namespace NYT
