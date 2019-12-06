#include "config.h"
#include "profile_manager.h"
#include "resource_tracker.h"
#include "timing.h"

#include <yt/core/concurrency/fork_aware_spinlock.h>
#include <yt/core/concurrency/periodic_executor.h>
#include <yt/core/concurrency/scheduler_thread.h>
#include <yt/core/concurrency/invoker_queue.h>

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

////////////////////////////////////////////////////////////////////////////////

class TProfileManager::TImpl
    : public TRefCounted
{
public:
    TImpl()
        : WasStarted_(false)
        , WasShutdown_(false)
        , EventQueue_(New<TInvokerQueue>(
            EventCount_,
            EmptyTagIds,
            true,
            false))
        , Thread_(New<TThread>(this))
        , Root_(GetEphemeralNodeFactory(true)->CreateMap())
        , EnqueuedCounter_("/enqueued")
        , DequeuedCounter_("/dequeued")
        , DroppedCounter_("/dropped")
    {
        ResourceTracker_ = New<TResourceTracker>(GetInvoker());
    }

    void Start()
    {
        YT_VERIFY(!WasStarted_);
        YT_VERIFY(!WasShutdown_);

        WasStarted_ = true;

        Thread_->Start();
        EventQueue_->SetThreadId(Thread_->GetId());

        DequeueExecutor_ = New<TPeriodicExecutor>(
            EventQueue_,
            BIND(&TImpl::OnDequeue, MakeStrong(this)),
            Config_->DequeuePeriod);
        DequeueExecutor_->Start();

#ifdef _linux_
        ResourceTracker_->Start();
#endif
    }

    void Shutdown()
    {
        WasShutdown_ = true;
        EventQueue_->Shutdown();
        Thread_->Shutdown();
    }

    void Enqueue(const TQueuedSample& sample, bool selfProfiling)
    {
        if (!WasStarted_ || WasShutdown_) {
            return;
        }

        if (!selfProfiling) {
            ProfilingProfiler.Increment(EnqueuedCounter_);
        }

        SampleQueue_.Enqueue(sample);
    }

    void Configure(const TProfileManagerConfigPtr& config)
    {
        GlobalTags_ = config->GlobalTags;
        Config_ = config;
    }

    IInvokerPtr GetInvoker() const
    {
        return EventQueue_;
    }

    IMapNodePtr GetRoot() const
    {
        return Root_;
    }

    IYPathServicePtr GetService() const
    {
        return GetRoot()->Via(GetInvoker());
    }

    TProfileManagerConfigPtr GetConfig() const
    {
        return Config_;
    }

    TTagId RegisterTag(const TTag& tag)
    {
        TGuard<TForkAwareSpinLock> guard(TagSpinLock_);
        auto pair = std::make_pair(tag.Key, tag.Value);
        auto it = TagToId_.find(pair);
        if (it != TagToId_.end()) {
            return it->second;
        }

        auto id = static_cast<TTagId>(IdToTag_.size());
        IdToTag_.push_back(tag);
        YT_VERIFY(TagToId_.insert(std::make_pair(pair, id)).second);
        TagKeyToValues_[tag.Key].push_back(tag.Value);

        return id;
    }

    TForkAwareSpinLock& GetTagSpinLock()
    {
        return TagSpinLock_;
    }

    const TTag& GetTag(TTagId id)
    {
        return IdToTag_[id];
    }

    void SetGlobalTag(TTagId id)
    {
        BIND([this_ = MakeStrong(this), this, id] () {
            const auto& newTag = GetTag(id);
            for (auto& tagId : GlobalTagIds_) {
                // Replace tag with the same key.
                if (GetTag(tagId).Key == newTag.Key) {
                    tagId = id;
                    return;
                }
            }
            GlobalTagIds_.push_back(id);
        })
            .Via(GetInvoker())
            .Run();
    }

    std::pair<i64, NProto::TPointBatch> GetSamples(std::optional<i64> count = std::nullopt)
    {
        auto result = BIND(&TSampleStorage::GetProtoSamples, &Storage_, count)
            .AsyncVia(GetInvoker()).Run();
        return WaitFor(result).ValueOrThrow();
    }

    TResourceTrackerPtr GetResourceTracker() const
    {
        return ResourceTracker_;
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
        using TSamples = std::deque<TStoredSample>;
        using TSamplesIterator = TSamples::iterator;
        using TSampleRange = TIteratorRange<TSamplesIterator>;
        using TSampleIdAndRange = std::pair<i64, TSampleRange>;

        //! Adds new sample to the deque.
        void AddSample(const TStoredSample& sample)
        {
            Samples_.push_back(sample);
        }

        //! Gets samples with #id more than #count in the deque and id of the first returned element.
        //! If #count is not given, all deque is returned.
        TSampleIdAndRange GetSamples(std::optional<i64> count = std::nullopt)
        {
            if (!count) {
                return std::make_pair(RemovedCount_, TSampleRange(Samples_.begin(), Samples_.end()));
            }
            if (*count > Samples_.size() + RemovedCount_) {
                return std::make_pair(*count, TSampleRange(Samples_.end(), Samples_.end()));
            } else {
                auto startIndex = std::max(*count - RemovedCount_, i64(0));
                return std::make_pair(startIndex + RemovedCount_,
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
                ++RemovedCount_;
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
                for (auto& [tagId, tag] : TagIdToValue_) {
                    tag = profilingManager->GetTag(tagId);
                }
            }

            NProto::TPointBatch protoVec;
            for (const auto& sample : samples) {
                auto* protoSample = protoVec.add_points();
                protoSample->set_time(ToProto<i64>(sample.Time));
                protoSample->set_value(sample.Value);
                ToProto(protoSample->mutable_tag_ids(), sample.TagIds);
                protoSample->set_metric_type(static_cast<NProfiling::NProto::EMetricType>(sample.MetricType));
                ToProto(protoSample->mutable_path(), sample.Path);
            }
            for (const auto& [id, tag] : TagIdToValue_) {
                auto* sample = protoVec.add_tags();
                sample->set_tag_id(id);
                ToProto(sample->mutable_key(), tag.Key);
                ToProto(sample->mutable_value(), tag.Value);
            }
            return {index, protoVec};
        }

    private:
        TSamples Samples_;
        THashMap<TTagId, TTag> TagIdToValue_;
        i64 RemovedCount_ = 0;
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
                                    const auto& tag = GetOrCrash(tagIdToValue, tagId);
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
                owner->EventCount_,
                "Profiling",
                EmptyTagIds,
                true,
                false)
            , Owner(owner)
        { }

    private:
        TImpl* const Owner;

        virtual TClosure BeginExecute() override
        {
            return Owner->BeginExecute();
        }

        virtual void EndExecute() override
        {
            Owner->EndExecute();
        }
    };

    const std::shared_ptr<TEventCount> EventCount_ = std::make_shared<TEventCount>();
    std::atomic<bool> WasStarted_;
    std::atomic<bool> WasShutdown_;
    TInvokerQueuePtr EventQueue_;
    TIntrusivePtr<TThread> Thread_;
    TEnqueuedAction CurrentAction_;

    TPeriodicExecutorPtr DequeueExecutor_;

    IMapNodePtr Root_;
    TMonotonicCounter EnqueuedCounter_;
    TMonotonicCounter DequeuedCounter_;
    TMonotonicCounter DroppedCounter_;

    TProfileManagerConfigPtr Config_;

    TMultipleProducerSingleConsumerLockFreeStack<TQueuedSample> SampleQueue_;
    THashMap<TYPath, TBucketPtr> PathToBucket_;
    TIdGenerator SampleIdGenerator_;

    TForkAwareSpinLock TagSpinLock_;
    std::vector<TTag> IdToTag_;
    THashMap<std::pair<TString, TString>, int> TagToId_;
    using TTagKeyToValues = THashMap<TString, std::vector<TString>>;
    TTagKeyToValues TagKeyToValues_;

    //! Tags attached to every sample.
    TTagIdList GlobalTagIds_;

    //! One deque instead of buckets with deques.
    TSampleStorage Storage_;

    TIntrusivePtr<TResourceTracker> ResourceTracker_;

    THashMap<TString, TString> GlobalTags_;

    TClosure BeginExecute()
    {
        return EventQueue_->BeginExecute(&CurrentAction_);
    }

    void EndExecute()
    {
        EventQueue_->EndExecute(&CurrentAction_);
    }

    void OnDequeue()
    {
        // Process all pending samples in a row.
        int samplesProcessed = 0;

        while (SampleQueue_.DequeueAll(true, [&] (TQueuedSample& sample) {
                // Enrich sample with global tags.
                sample.TagIds.insert(sample.TagIds.end(), GlobalTagIds_.begin(), GlobalTagIds_.end());
                ProcessSample(sample);
                ProcessSampleV2(sample);
                ++samplesProcessed;
            }))
        { }

        ProfilingProfiler.Increment(DequeuedCounter_, samplesProcessed);
    }

    TBucketPtr LookupBucket(const TYPath& path)
    {
        auto it = PathToBucket_.find(path);
        if (it != PathToBucket_.end()) {
            return it->second;
        }

        YT_LOG_DEBUG("Creating bucket %v", path);
        auto bucket = New<TBucket>(GlobalTags_, Config_);
        YT_VERIFY(PathToBucket_.insert(std::make_pair(path, bucket)).second);

        auto node = CreateVirtualNode(bucket);
        ForceYPath(Root_, path);
        SetNodeByYPath(Root_, path, node);

        return bucket;
    }

    void ProcessSample(const TQueuedSample& queuedSample)
    {
        auto bucket = LookupBucket(queuedSample.Path);

        TStoredSample storedSample;
        storedSample.Id = SampleIdGenerator_.Next();
        storedSample.Time = CpuInstantToInstant(queuedSample.Time);
        storedSample.Value = queuedSample.Value;
        storedSample.TagIds = queuedSample.TagIds;
        storedSample.MetricType = queuedSample.MetricType;

        if (bucket->AddSample(storedSample) == 1) {
            THashMultiMap<TString, TString> tags;
            {
                TGuard<TForkAwareSpinLock> guard(TagSpinLock_);
                for (auto tagId : storedSample.TagIds) {
                    const auto& tag = GetTag(tagId);
                    tags.insert(std::make_pair(tag.Key, tag.Value));
                }
            }
            YT_LOG_DEBUG("Profiling sample dropped (Path: %v, Tags: %v)",
                queuedSample.Path,
                tags);
            ProfilingProfiler.Increment(DroppedCounter_);
        }
        bucket->TrimSamples(Config_->MaxKeepInterval);
    }

    void ProcessSampleV2(const TQueuedSample& queuedSample)
    {
        TStoredSample storedSample;
        storedSample.Time = CpuInstantToInstant(queuedSample.Time);
        storedSample.Value = queuedSample.Value;
        storedSample.TagIds = queuedSample.TagIds;
        storedSample.MetricType = queuedSample.MetricType;
        storedSample.Path = queuedSample.Path;
        Storage_.AddSample(storedSample);
        Storage_.RemoveOldSamples(Config_->MaxKeepInterval);
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

void TProfileManager::SetGlobalTag(TTagId id)
{
    Impl_->SetGlobalTag(id);
}

std::pair<i64, NProto::TPointBatch> TProfileManager::GetSamples(std::optional<i64> count)
{
    return Impl_->GetSamples(count);
}

TResourceTrackerPtr TProfileManager::GetResourceTracker() const
{
    return Impl_->GetResourceTracker();
}

////////////////////////////////////////////////////////////////////////////////

REGISTER_SHUTDOWN_CALLBACK(4, TProfileManager::StaticShutdown);

} // namespace NYT::NProfiling
