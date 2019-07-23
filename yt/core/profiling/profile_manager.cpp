#include "config.h"
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

#include <yt/core/profiling/proto/profiling.pb.h>

#include <util/generic/iterator_range.h>

namespace NYT::NProfiling {

using namespace NYTree;
using namespace NYson;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

static NLogging::TLogger Logger("Profiling");
static TProfiler ProfilingProfiler("/profiling", EmptyTagIds, true);
// TODO(babenko): make configurable

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
        , DroppedCounter("/dropped")
    {
#ifdef _linux_
        ResourceTracker = New<TResourceTracker>(GetInvoker());
#endif
    }

    void Start()
    {
        YT_VERIFY(!WasStarted);
        YT_VERIFY(!WasShutdown);

        WasStarted = true;

        Thread->Start();
        EventQueue->SetThreadId(Thread->GetId());

        DequeueExecutor = New<TPeriodicExecutor>(
            EventQueue,
            BIND(&TImpl::OnDequeue, MakeStrong(this)),
            Config->DequeuePeriod);
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

    void Configure(const TProfileManagerConfigPtr& config)
    {
        GlobalTags_ = config->GlobalTags;
        Config = config;
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

    TProfileManagerConfigPtr GetConfig() const
    {
        return Config;
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
        YT_VERIFY(TagToId.insert(std::make_pair(pair, id)).second);
        TagKeyToValues[tag.Key].push_back(tag.Value);

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

    std::pair<i64, NProto::TPointBatch> GetSamples(std::optional<i64> count = std::nullopt)
    {
        auto result = BIND(&TSampleStorage::GetProtoSamples, &Storage, count)
            .AsyncVia(GetInvoker()).Run();
        return WaitFor(result).ValueOrThrow();
    }

private:
    struct TStoredSample
    {
        i64 Id;
        TInstant Time;
        TValue Value;
        TTagIdList TagIds;
        EMetricType MetricType;
        NYPath::TYPath Path;
    };

    class TSampleStorage
    {
    public:
        typedef std::deque<TStoredSample> TSamples;
        typedef TSamples::iterator TSamplesIterator;
        typedef TIteratorRange<TSamplesIterator> TSampleRange;
        typedef std::pair<i64, TSampleRange> TSampleIdAndRange;

        //! Adds new sample to the deque
        void AddSample(const TStoredSample& sample)
        {
            Samples_.push_back(sample);
        }

        //! Gets samples with #id more than #count in the deque and id of the first returned element.
        //! If #count is not given, all deque is returned.
        TSampleIdAndRange GetSamples(std::optional<i64> count = std::nullopt)
        {
            if (!count) {
                return std::make_pair(removed, TSampleRange(Samples_.begin(), Samples_.end()));
            }
            if (*count > Samples_.size() + removed) {
                return std::make_pair(*count, TSampleRange(Samples_.end(), Samples_.end()));
            } else {
                auto startIndex = std::max(*count - removed, i64(0));
                return std::make_pair(startIndex + removed,
                    TSampleRange(Samples_.begin() + startIndex, Samples_.end()));
            }
        }

        //! Removes old samples (#count instances from the beginning of the deque).
        void RemoveOldSamples(TDuration maxKeepInterval)
        {
            if (Samples_.size() <= 1) {
                return;
            }
            auto deadline = Samples_.back().Time - maxKeepInterval;
            while (Samples_.front().Time < deadline) {
                ++removed;
                Samples_.pop_front();
            }
        }

        std::pair<i64, NProto::TPointBatch> GetProtoSamples(std::optional<i64> count = std::nullopt)
        {
            auto [index, samples] = GetSamples(count);

            for (const auto& sample : samples) {
                for (auto tagId : sample.TagIds) {
                    TagIdToValue_.emplace(tagId, TTag());
                }
            }

            {
                const auto& profilingManager = TProfileManager::Get()->Impl_;
                TGuard<TForkAwareSpinLock> tagGuard(profilingManager->GetTagSpinLock());
                for (auto& pair : TagIdToValue_) {
                    pair.second = profilingManager->GetTag(pair.first);
                }
            }

            NProto::TPointBatch protoVec;
            for (const auto& sample : samples) {
                NProto::TPoint *protoSample = protoVec.add_points();

                protoSample->set_time(ToProto<i64>(sample.Time));
                protoSample->set_value(sample.Value);
                ToProto(protoSample->mutable_tag_ids(), sample.TagIds);
                protoSample->set_metric_type(static_cast<NYT::NProfiling::NProto::EMetricType>(sample.MetricType));
                ToProto(protoSample->mutable_path(), sample.Path);
            }
            for (const auto& [id, tag] : TagIdToValue_) {
                NProto::TTag *sample = protoVec.add_tags();
                sample->set_tag_id(id);
                ToProto(sample->mutable_key(), tag.Key);
                ToProto(sample->mutable_value(), tag.Value);
            }
            return {index, protoVec};
        }

    private:
        TSamples Samples_;
        THashMap<TTagId, TTag> TagIdToValue_;
        i64 removed;
    };

    class TBucket
        : public TYPathServiceBase
        , public TSupportsGet
    {
    public:
        typedef std::deque<TStoredSample> TSamples;
        typedef TSamples::iterator TSamplesIterator;
        typedef std::pair<TSamplesIterator, TSamplesIterator> TSamplesRange;

        TBucket(const THashMap<TString, TString>& globalTags, const TProfileManagerConfigPtr& config)
            : GlobalTags_(globalTags)
            , Config_(config)
        { }

        //! Adds a new sample to the bucket inserting in at an appropriate position.
        int AddSample(const TStoredSample& sample)
        {
            auto& rateLimit = RateLimits[sample.TagIds];
            if (rateLimit.first && sample.Time < rateLimit.first + Config_->SampleRateLimit) {
                ++rateLimit.second;
                return rateLimit.second;
            }
            rateLimit.first = sample.Time;
            rateLimit.second = 0;

            // Samples are ordered by time.
            // Search for an appropriate insertion point starting from the the back,
            // this should usually be fast.
            int index = static_cast<int>(Samples.size());
            while (index > 0 && Samples[index - 1].Time > sample.Time) {
                --index;
            }
            Samples.insert(Samples.begin() + index, sample);
            return 0;
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
        //! If #lastTime is null then all samples are returned.
        TSamplesRange GetSamples(std::optional<TInstant> lastTime = std::nullopt)
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
        std::map<TTagIdList, std::pair<TInstant, int>> RateLimits;
        THashMap<TString, TString> GlobalTags_;
        TProfileManagerConfigPtr Config_;

        virtual bool DoInvoke(const NRpc::IServiceContextPtr& context) override
        {
            DISPATCH_YPATH_SERVICE_METHOD(Get);
            return TYPathServiceBase::DoInvoke(context);
        }

        static std::optional<TInstant> ParseInstant(std::optional<i64> value)
        {
            return value ? std::make_optional(TInstant::MicroSeconds(*value)) : std::nullopt;
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
                            .Item("tags").BeginMap()
                                .DoFor(GlobalTags_, [&] (TFluentMap fluent, const std::pair<const TString, TString>& tag) {
                                    fluent
                                        .Item(tag.first).Value(tag.second);
                                })
                                .DoFor(sample.TagIds, [&] (TFluentMap fluent, TTagId tagId) {
                                    auto it = tagIdToValue.find(tagId);
                                    YT_VERIFY(it != tagIdToValue.end());
                                    const auto& tag = it->second;
                                    fluent
                                        .Item(tag.Key).Value(tag.Value);
                                })
                            .EndMap()
                            .Item("metric_type").Value(sample.MetricType)
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
    std::atomic<bool> WasStarted;
    std::atomic<bool> WasShutdown;
    TInvokerQueuePtr EventQueue;
    TIntrusivePtr<TThread> Thread;
    TEnqueuedAction CurrentAction;

    TPeriodicExecutorPtr DequeueExecutor;

    IMapNodePtr Root;
    TMonotonicCounter EnqueuedCounter;
    TMonotonicCounter DequeuedCounter;
    TMonotonicCounter DroppedCounter;

    TProfileManagerConfigPtr Config;

    TMultipleProducerSingleConsumerLockFreeStack<TQueuedSample> SampleQueue;
    THashMap<TYPath, TBucketPtr> PathToBucket;
    TIdGenerator SampleIdGenerator;

    TForkAwareSpinLock TagSpinLock;
    std::vector<TTag> IdToTag;
    THashMap<std::pair<TString, TString>, int> TagToId;
    typedef THashMap<TString, std::vector<TString>> TTagKeyToValues;
    TTagKeyToValues TagKeyToValues;

    //! One deque instead of buckets with deques
    TSampleStorage Storage;

#ifdef _linux_
    TIntrusivePtr<TResourceTracker> ResourceTracker;
#endif

    THashMap<TString, TString> GlobalTags_;

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
                ProcessSampleV2(sample);
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

        YT_LOG_DEBUG("Creating bucket %v", path);
        auto bucket = New<TBucket>(GlobalTags_, Config);
        YT_VERIFY(PathToBucket.insert(std::make_pair(path, bucket)).second);

        auto node = CreateVirtualNode(bucket);
        ForceYPath(Root, path);
        SetNodeByYPath(Root, path, node);

        return bucket;
    }

    void ProcessSample(const TQueuedSample& queuedSample)
    {
        auto bucket = LookupBucket(queuedSample.Path);

        TStoredSample storedSample;
        storedSample.Id = SampleIdGenerator.Next();
        storedSample.Time = CpuInstantToInstant(queuedSample.Time);
        storedSample.Value = queuedSample.Value;
        storedSample.TagIds = queuedSample.TagIds;
        storedSample.MetricType = queuedSample.MetricType;

        if (bucket->AddSample(storedSample) == 1) {
            THashMultiMap<TString, TString> tags;
            {
                TGuard<TForkAwareSpinLock> guard(TagSpinLock);
                for (auto tagId : storedSample.TagIds) {
                    const auto& tag = GetTag(tagId);
                    tags.insert(std::make_pair(tag.Key, tag.Value));
                }
            }
            YT_LOG_DEBUG("Profiling sample dropped (Path: %v, Tags: %v)",
                queuedSample.Path,
                tags);
            ProfilingProfiler.Increment(DroppedCounter);
        }
        bucket->TrimSamples(Config->MaxKeepInterval);
    }

    void ProcessSampleV2(const TQueuedSample& queuedSample)
    {
        TStoredSample storedSample;
        storedSample.Time = CpuInstantToInstant(queuedSample.Time);
        storedSample.Value = queuedSample.Value;
        storedSample.TagIds = queuedSample.TagIds;
        storedSample.MetricType = queuedSample.MetricType;
        storedSample.Path = queuedSample.Path;
        Storage.AddSample(storedSample);
        Storage.RemoveOldSamples(Config->MaxKeepInterval);
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

void TProfileManager::Configure(const TProfileManagerConfigPtr& config)
{
    Impl_->Configure(config);
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

std::pair<i64, NProto::TPointBatch> TProfileManager::GetSamples(std::optional<i64> count) {
    return Impl_->GetSamples(count);
}

////////////////////////////////////////////////////////////////////////////////

REGISTER_SHUTDOWN_CALLBACK(4, TProfileManager::StaticShutdown);

} // namespace NYT::NProfiling
