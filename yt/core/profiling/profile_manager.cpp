#include "stdafx.h"
#include "profile_manager.h"
#include "resource_tracker.h"
#include "timing.h"

#include <core/concurrency/periodic_executor.h>
#include <core/concurrency/fork_aware_spinlock.h>

#include <core/misc/id_generator.h>
#include <core/misc/hash.h>

#include <core/concurrency/scheduler_thread.h>

#include <core/ytree/node.h>
#include <core/ytree/ephemeral_node_factory.h>
#include <core/ytree/ypath_detail.h>
#include <core/ytree/virtual.h>
#include <core/ytree/ypath_client.h>
#include <core/ytree/fluent.h>

#include <core/logging/log.h>

namespace NYT {
namespace NProfiling  {

using namespace NYTree;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger Logger("Profiling");
static TProfiler ProfilingProfiler("/profiling", EmptyTagIds, true);
// TODO(babenko): make configurable
static const TDuration MaxKeepInterval = TDuration::Minutes(5);

////////////////////////////////////////////////////////////////////////////////

class TProfileManager::TImpl
{
public:
    TImpl()
        : WasShutdown(false)
        , Queue(New<TInvokerQueue>(
            &EventCount,
            EmptyTagIds,
            true,
            false))
        , Thread(New<TThread>(this))
        , Root(GetEphemeralNodeFactory()->CreateMap())
        , EnqueueCounter("/enqueue_rate")
        , DequeueCounter("/dequeue_rate")
    {
#ifdef _linux_
        ResourceTracker = New<TResourceTracker>(GetInvoker());
#endif
    }

    void Start()
    {
        Thread->Start();
        Queue->SetThreadId(Thread->GetId());
#ifdef _linux_
        ResourceTracker->Start();
#endif
    }

    void Shutdown()
    {
        WasShutdown = true;
        Queue->Shutdown();
        Thread->Shutdown();
    }


    void Enqueue(const TQueuedSample& sample, bool selfProfiling)
    {
        if (WasShutdown)
            return;

        if (!selfProfiling) {
            ProfilingProfiler.Increment(EnqueueCounter);
        }

        SampleQueue.Enqueue(sample);
        EventCount.NotifyOne();
    }


    IInvokerPtr GetInvoker() const
    {
        return Queue;
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
                    .DoListFor(pair.second, [] (TFluentList fluent, const TYsonString& value) {
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

        virtual NLog::TLogger CreateLogger() const override
        {
            return NProfiling::Logger;
        }

        virtual bool DoInvoke(NRpc::IServiceContextPtr context) override
        {
            DISPATCH_YPATH_SERVICE_METHOD(Get);
            return TYPathServiceBase::DoInvoke(context);
        }

        static TNullable<TInstant> ParseInstant(TNullable<i64> value)
        {
            return value ? MakeNullable(TInstant::MicroSeconds(value.Get())) : Null;
        }

        virtual void GetSelf(TReqGet* request, TRspGet* response, TCtxGetPtr context)
        {
            auto* profilingManager = TProfileManager::Get()->Impl_.get();
            TGuard<TForkAwareSpinLock> tagGuard(profilingManager->GetTagSpinLock());

            context->SetRequestInfo();

            auto options = FromProto(request->options());
            auto fromTime = ParseInstant(options->Find<i64>("from_time"));
            auto range = GetSamples(fromTime);
            response->set_value(BuildYsonStringFluently()
                .DoListFor(range.first, range.second, [&] (TFluentList fluent, const TSamplesIterator& it) {
                    const auto& sample = *it;
                    fluent
                        .Item().BeginMap()
                            .Item("id").Value(sample.Id)
                            .Item("time").Value(static_cast<i64>(sample.Time.MicroSeconds()))
                            .Item("value").Value(sample.Value)
                            .Item("tags").DoMapFor(sample.TagIds, [&] (TFluentMap fluent, TTagId id) {
                                const auto& tag = profilingManager->GetTag(id);
                                fluent
                                    .Item(tag.Key).Value(tag.Value);
                            })
                        .EndMap();
                }).Data());

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
                &owner->EventCount,
                "Profiling",
                EmptyTagIds,
                true,
                false)
            , Owner(owner)
        { }

    private:
        TImpl* Owner;

        virtual EBeginExecuteResult BeginExecute() override
        {
            return Owner->BeginExecute();
        }

        virtual void EndExecute() override
        {
            Owner->EndExecute();
        }

    };


    TEventCount EventCount;
    volatile bool WasShutdown;
    TInvokerQueuePtr Queue;
    TIntrusivePtr<TThread> Thread;
    TEnqueuedAction CurrentAction;

    IMapNodePtr Root;
    TRateCounter EnqueueCounter;
    TRateCounter DequeueCounter;

    TLockFreeQueue<TQueuedSample> SampleQueue;
    yhash_map<TYPath, TBucketPtr> PathToBucket;
    TIdGenerator SampleIdGenerator;

    TForkAwareSpinLock TagSpinLock;
    std::vector<TTag> IdToTag;
    yhash_map<std::pair<Stroka, TYsonString>, int> TagToId;
    typedef yhash_map<Stroka, std::vector<TYsonString>> TTagKeyToValues;
    TTagKeyToValues TagKeyToValues;

#ifdef _linux_
    TIntrusivePtr<TResourceTracker> ResourceTracker;
#endif

    EBeginExecuteResult BeginExecute()
    {
        // Handle pending callbacks first.
        auto result = Queue->BeginExecute(&CurrentAction);
        if (result != EBeginExecuteResult::QueueEmpty) {
            return result;
        }

        // Process all pending samples in a row.
        int samplesProcessed = 0;
        TQueuedSample sample;
        while (SampleQueue.Dequeue(&sample)) {
            ProcessSample(sample);
            ++samplesProcessed;
        }

        ProfilingProfiler.Increment(DequeueCounter, samplesProcessed);

        if (samplesProcessed > 0) {
            EventCount.CancelWait();
            return EBeginExecuteResult::Success;
        } else {
            return EBeginExecuteResult::QueueEmpty;
        }
    }

    void EndExecute()
    {
        Queue->EndExecute(&CurrentAction);
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

        bucket->AddSample(storedSample);
        bucket->TrimSamples(MaxKeepInterval);
    }

};

////////////////////////////////////////////////////////////////////////////////

TProfileManager::TProfileManager()
    : Impl_(new TImpl())
{ }

TProfileManager::~TProfileManager()
{ }

TProfileManager* TProfileManager::Get()
{
    return Singleton<TProfileManager>();
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

} // namespace NProfiling
} // namespace NYT
