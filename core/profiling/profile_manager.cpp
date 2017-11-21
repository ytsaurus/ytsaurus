#include "profile_manager.h"
#include "resource_tracker.h"
#include "timing.h"

#include <yt/core/concurrency/fork_aware_spinlock.h>
#include <yt/core/concurrency/periodic_executor.h>
#include <yt/core/concurrency/scheduler_thread.h>

#include <yt/core/logging/log.h>

#include <yt/core/misc/hash.h>
#include <yt/core/misc/id_generator.h>
#include <yt/core/misc/lock_free.h>
#include <yt/core/misc/singleton.h>
#include <yt/core/misc/shutdown.h>

#include <yt/core/ytree/ephemeral_node_factory.h>
#include <yt/core/ytree/fluent.h>
#include <yt/core/ytree/node.h>
#include <yt/core/ytree/virtual.h>
#include <yt/core/ytree/ypath_client.h>
#include <yt/core/ytree/ypath_detail.h>

namespace NYT {
namespace NProfiling  {

using namespace NYTree;
using namespace NYson;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

static NLogging::TLogger Logger("Profiling");
static TProfiler ProfilingProfiler("/profiling", EmptyTagIds, true);
// TODO(babenko): make configurable
static const auto MaxKeepInterval = TDuration::Minutes(5);
static const auto DequeuePeriod = TDuration::MilliSeconds(100);

////////////////////////////////////////////////////////////////////////////////

class TProfileManager::TImpl
    : public TRefCounted
{
public:
    TImpl()
        : WasStarted(false)
        , WasShutdown(false)
        , EventQueue(New<TInvokerQueue>(
            EventCount,
            EmptyTagIds,
            true,
            false))
        , Thread(New<TThread>(this))
        , Root(GetEphemeralNodeFactory(true)->CreateMap())
        , EnqueuedCounter("/enqueued")
        , DequeuedCounter("/dequeued")
    {
#ifdef _linux_
        ResourceTracker = New<TResourceTracker>(GetInvoker());
#endif
    }

    void Start()
    {
        YCHECK(!WasStarted);
        YCHECK(!WasShutdown);

        WasStarted = true;

        Thread->Start();
        EventQueue->SetThreadId(Thread->GetId());

        DequeueExecutor = New<TPeriodicExecutor>(
            EventQueue,
            BIND(&TImpl::OnDequeue, MakeStrong(this)),
            DequeuePeriod);
        DequeueExecutor->Start();

#ifdef _linux_
        ResourceTracker->Start();
#endif
    }

    void Shutdown()
    {
        WasShutdown = true;
        EventQueue->Shutdown();
        Thread->Shutdown();
    }


    void Enqueue(const TQueuedSample& sample, bool selfProfiling)
    {
        if (!WasStarted || WasShutdown) {
            return;
        }

        if (!selfProfiling) {
            ProfilingProfiler.Increment(EnqueuedCounter);
        }

        SampleQueue.Enqueue(sample);
    }


    IInvokerPtr GetInvoker() const
    {
        return EventQueue;
    }

    IMapNodePtr GetRoot() const
    {
        return Root;
    }

    IYPathServicePtr GetService() const
    {
        return GetRoot()->Via(GetInvoker());
    }

    TTagId RegisterTag(const TTag& tag)
    {
        TGuard<TForkAwareSpinLock> guard(TagSpinLock);
        auto pair = std::make_pair(tag.Key, tag.Value);
        auto it = TagToId.find(pair);
        if (it != TagToId.end()) {
            return it->second;
        }
        
        auto id = static_cast<TTagId>(IdToTag.size());
        IdToTag.push_back(tag);
        YCHECK(TagToId.insert(std::make_pair(pair, id)).second);

        TagKeyToValues[tag.Key].push_back(tag.Value);

        auto tags = BuildYsonStringFluently()
            .DoMapFor(TagKeyToValues, [] (TFluentMap fluent, const TTagKeyToValues::value_type& pair) {
                fluent
                    .Item(pair.first)
                    .DoListFor(pair.second, [] (TFluentList fluent, const TString& value) {
                        fluent
                            .Item().Value(value);
                    });
            });
        SyncYPathSet(Root, "/@tags", tags);

        return id;
    }

    TForkAwareSpinLock& GetTagSpinLock()
    {
        return TagSpinLock;
    }

    const TTag& GetTag(TTagId id)
    {
        return IdToTag[id];
    }

private:
    struct TStoredSample
    {
        i64 Id;
        TInstant Time;
        TValue Value;
        TTagIdList TagIds;
        EMetricType MetricType;
    };

    class TBucket
        : public TYPathServiceBase
        , public TSupportsGet
    {
    public:
        typedef std::deque<TStoredSample> TSamples;
        typedef TSamples::iterator TSamplesIterator;
        typedef std::pair<TSamplesIterator, TSamplesIterator> TSamplesRange;

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
            lastSample.Time = *lastTime;
            auto it = std::upper_bound(
                Samples.begin(),
                Samples.end(),
                lastSample,
                [=] (const TStoredSample& lhs, const TStoredSample& rhs) { return lhs.Time < rhs.Time; });

            return std::make_pair(it, Samples.end());
        }

    private:
        std::deque<TStoredSample> Samples;

        virtual bool DoInvoke(const NRpc::IServiceContextPtr& context) override
        {
            DISPATCH_YPATH_SERVICE_METHOD(Get);
            return TYPathServiceBase::DoInvoke(context);
        }

        static TNullable<TInstant> ParseInstant(TNullable<i64> value)
        {
            return value ? MakeNullable(TInstant::MicroSeconds(*value)) : Null;
        }

        virtual void GetSelf(
            TReqGet* request,
            TRspGet* response,
            const TCtxGetPtr& context)
        {
            auto options = FromProto(request->options());
            auto fromTime = ParseInstant(options->Find<i64>("from_time"));
            context->SetRequestInfo("FromTime: %v", fromTime);

            auto samplesRange = GetSamples(fromTime);

            THashMap<TTagId, TTag> tagIdToValue;
            for (auto it = samplesRange.first; it != samplesRange.second; ++it) {
                const auto& sample = *it;
                for (auto tagId : sample.TagIds) {
                    tagIdToValue.emplace(tagId, TTag());
                }
            }

            {
                const auto& profilingManager = TProfileManager::Get()->Impl_;
                TGuard<TForkAwareSpinLock> tagGuard(profilingManager->GetTagSpinLock());
                for (auto& pair : tagIdToValue) {
                    pair.second = profilingManager->GetTag(pair.first);
                }
            }

            response->set_value(BuildYsonStringFluently()
                .DoListFor(samplesRange.first, samplesRange.second, [&] (TFluentList fluent, const TSamplesIterator& it) {
                    const auto& sample = *it;
                    fluent
                        .Item().BeginMap()
                            .Item("id").Value(sample.Id)
                            .Item("time").Value(static_cast<i64>(sample.Time.MicroSeconds()))
                            .Item("value").Value(sample.Value)
                            .Item("tags").DoMapFor(sample.TagIds, [&] (TFluentMap fluent, TTagId tagId) {
                                auto it = tagIdToValue.find(tagId);
                                YCHECK(it != tagIdToValue.end());
                                const auto& tag = it->second;
                                fluent
                                    .Item(tag.Key).Value(tag.Value);
                            })
                            .Item("metric_type").Value(FormatEnum(sample.MetricType))
                        .EndMap();
                }).GetData());

            context->Reply();
        }
    };

    typedef TIntrusivePtr<TBucket> TBucketPtr;


    class TThread
        : public TSchedulerThread
    {
    public:
        explicit TThread(TImpl* owner)
            : TSchedulerThread(
                owner->EventCount,
                "Profiling",
                EmptyTagIds,
                true,
                false)
            , Owner(owner)
        { }

    private:
        TImpl* const Owner;

        virtual EBeginExecuteResult BeginExecute() override
        {
            return Owner->BeginExecute();
        }

        virtual void EndExecute() override
        {
            Owner->EndExecute();
        }
    };

    const std::shared_ptr<TEventCount> EventCount = std::make_shared<TEventCount>();
    volatile bool WasStarted;
    volatile bool WasShutdown;
    TInvokerQueuePtr EventQueue;
    TIntrusivePtr<TThread> Thread;
    TEnqueuedAction CurrentAction;

    TPeriodicExecutorPtr DequeueExecutor;

    IMapNodePtr Root;
    TSimpleCounter EnqueuedCounter;
    TSimpleCounter DequeuedCounter;

    TMultipleProducerSingleConsumerLockFreeStack<TQueuedSample> SampleQueue;
    THashMap<TYPath, TBucketPtr> PathToBucket;
    TIdGenerator SampleIdGenerator;

    TForkAwareSpinLock TagSpinLock;
    std::vector<TTag> IdToTag;
    THashMap<std::pair<TString, TString>, int> TagToId;
    typedef THashMap<TString, std::vector<TString>> TTagKeyToValues;
    TTagKeyToValues TagKeyToValues;

#ifdef _linux_
    TIntrusivePtr<TResourceTracker> ResourceTracker;
#endif

    EBeginExecuteResult BeginExecute()
    {
        return EventQueue->BeginExecute(&CurrentAction);
    }

    void EndExecute()
    {
        EventQueue->EndExecute(&CurrentAction);
    }


    void OnDequeue()
    {
        // Process all pending samples in a row.
        int samplesProcessed = 0;

        while (SampleQueue.DequeueAll(true, [&] (TQueuedSample& sample) {
                ProcessSample(sample);
                ++samplesProcessed;
            }))
        { }

        ProfilingProfiler.Increment(DequeuedCounter, samplesProcessed);
    }


    TBucketPtr LookupBucket(const TYPath& path)
    {
        auto it = PathToBucket.find(path);
        if (it != PathToBucket.end()) {
            return it->second;
        }

        LOG_DEBUG("Creating bucket %v", path);
        auto bucket = New<TBucket>();
        YCHECK(PathToBucket.insert(std::make_pair(path, bucket)).second);

        auto node = CreateVirtualNode(bucket);
        ForceYPath(Root, path);
        SetNodeByYPath(Root, path, node);

        return bucket;
    }

    void ProcessSample(TQueuedSample& queuedSample)
    {
        auto bucket = LookupBucket(queuedSample.Path);

        TStoredSample storedSample;
        storedSample.Id = SampleIdGenerator.Next();
        storedSample.Time = CpuInstantToInstant(queuedSample.Time);
        storedSample.Value = queuedSample.Value;
        storedSample.TagIds = queuedSample.TagIds;
        storedSample.MetricType = queuedSample.MetricType;

        bucket->AddSample(storedSample);
        bucket->TrimSamples(MaxKeepInterval);
    }
};

////////////////////////////////////////////////////////////////////////////////

TProfileManager::TProfileManager()
    : Impl_(New<TImpl>())
{ }

TProfileManager::~TProfileManager() = default;

TProfileManager* TProfileManager::Get()
{
    return Singleton<TProfileManager>();
}

void TProfileManager::StaticShutdown()
{
    Get()->Shutdown();
}

void TProfileManager::Start()
{
    Impl_->Start();
}

void TProfileManager::Shutdown()
{
    Impl_->Shutdown();
}

void TProfileManager::Enqueue(const TQueuedSample& sample, bool selfProfiling)
{
    Impl_->Enqueue(sample, selfProfiling);
}

IInvokerPtr TProfileManager::GetInvoker() const
{
    return Impl_->GetInvoker();
}

IMapNodePtr TProfileManager::GetRoot() const
{
    return Impl_->GetRoot();
}

IYPathServicePtr TProfileManager::GetService() const
{
    return Impl_->GetService();
}

TTagId TProfileManager::RegisterTag(const TTag& tag)
{
    return Impl_->RegisterTag(tag);
}

////////////////////////////////////////////////////////////////////////////////

REGISTER_SHUTDOWN_CALLBACK(4, TProfileManager::StaticShutdown);

} // namespace NProfiling
} // namespace NYT
