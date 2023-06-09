#include "io_tracker.h"

#include "private.h"
#include "config.h"

#include <yt/yt/core/tracing/trace_context.h>

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/delayed_executor.h>
#include <yt/yt/core/concurrency/periodic_yielder.h>

#include <yt/yt/core/misc/mpsc_stack.h>

#include <yt/yt/core/logging/fluent_log.h>

#include <yt/yt/core/yson/pull_parser.h>

#include <library/cpp/yt/memory/atomic_intrusive_ptr.h>

namespace NYT::NIO {

using namespace NYTree;
using namespace NConcurrency;
using namespace NLogging;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

static const TString PathTagName = "object_path";

////////////////////////////////////////////////////////////////////////////////

void TIOCounters::MergeFrom(TIOCounters other)
{
    Bytes += other.Bytes;
    IORequests += other.IORequests;
}

////////////////////////////////////////////////////////////////////////////////

struct TSortedIOTagList
{
    TIOTagList Tags;

    std::optional<TString> FindTag(const TString& key) const
    {
        auto iter = std::lower_bound(Tags.begin(), Tags.end(), std::make_pair(key, TString()));
        if (iter == Tags.end() || iter->first != key) {
            return std::nullopt;
        }
        return iter->second;
    }

    template <typename TFilter>
    TSortedIOTagList FilterTags(TFilter filter) const
    {
        TSortedIOTagList result;
        result.Tags.reserve(Tags.size());
        std::copy_if(Tags.begin(), Tags.end(), std::back_inserter(result.Tags), filter);
        return result;
    }

    static TSortedIOTagList FromTagList(TIOTagList srcTags)
    {
        // NB. For the sake of clarity, we want to retain only the first tag if there are any duplicates. So,
        // we do std::stable_sort instead of std::sort.
        std::stable_sort(srcTags.begin(), srcTags.end());
        auto newTagsEnd = std::unique(
            srcTags.begin(),
            srcTags.end(),
            [] (const auto& lhs, const auto& rhs) { return lhs.first == rhs.first; });
        srcTags.erase(newTagsEnd, srcTags.end());
        return TSortedIOTagList{std::move(srcTags)};
    }

    bool operator == (const TSortedIOTagList& other) const
    {
        return Tags == other.Tags;
    }
};

struct TAggregateTagsKey
{
    TSortedIOTagList InlineTags;
    std::optional<TSortedIOTagList> NestedTags;

    bool operator == (const TAggregateTagsKey& other) const
    {
        return InlineTags == other.InlineTags && NestedTags == other.NestedTags;
    }
};

////////////////////////////////////////////////////////////////////////////////

}  // namespace NYT::NIO

template <>
struct THash<NYT::NIO::TSortedIOTagList>
{
    size_t operator()(const NYT::NIO::TSortedIOTagList& tags)
    {
        size_t hash = 0;
        for (const auto& [key, value] : tags.Tags) {
            NYT::HashCombine(hash, key);
            NYT::HashCombine(hash, value);
        }
        return hash;
    }
};

template <>
struct THash<NYT::NIO::TAggregateTagsKey>
{
    size_t operator()(const NYT::NIO::TAggregateTagsKey& tags)
    {
        size_t hash = 0;
        NYT::HashCombine(hash, tags.InlineTags);
        NYT::HashCombine(hash, tags.NestedTags);
        return hash;
    }
};

namespace NYT::NIO {

////////////////////////////////////////////////////////////////////////////////

struct TParsedIOEvent
{
    TIOCounters Counters;
    TSortedIOTagList AggregatingTags;
    TSortedIOTagList NonAggregatingTags;
};

////////////////////////////////////////////////////////////////////////////////

bool IsTagAggregating(const TString& tagName)
{
    return tagName.EndsWith("@");
}

template <typename TFunc>
void ParseBaggage(const TYsonString& baggage, TFunc addTag)
{
    if (!baggage) {
        return;
    }
    TMemoryInput in(baggage.AsStringBuf());
    TYsonPullParser parser(&in, EYsonType::Node);
    TYsonPullParserCursor cursor(&parser);
    cursor.ParseMap([&] (auto* cursor) {
        EnsureYsonToken("tag key", *cursor, EYsonItemType::StringValue);
        TString key{(*cursor)->UncheckedAsString()};
        cursor->Next();
        if ((*cursor)->GetType() != EYsonItemType::StringValue) {
            cursor->SkipComplexValue();
            return;
        }
        addTag(std::move(key), TString{(*cursor)->UncheckedAsString()});
        cursor->Next();
    });
}

TParsedIOEvent ParseEvent(const TIOEvent& ioEvent)
{
    TIOTagList aggregatingTags;
    TIOTagList nonAggregatingTags;

    auto addTag = [&] (TString key, TString value) {
        auto& targetList = IsTagAggregating(key) ? aggregatingTags : nonAggregatingTags;
        targetList.emplace_back(std::move(key), std::move(value));
    };

    for (auto tag : ioEvent.LocalTags) {
        addTag(std::move(tag.first), std::move(tag.second));
    }
    ParseBaggage(ioEvent.Baggage, addTag);

    return TParsedIOEvent{
        .Counters = ioEvent.Counters,
        .AggregatingTags = TSortedIOTagList::FromTagList(std::move(aggregatingTags)),
        .NonAggregatingTags = TSortedIOTagList::FromTagList(std::move(nonAggregatingTags))
    };
}

std::vector<TParsedIOEvent> ParseEvents(const std::vector<TIOEvent>& ioEvents)
{
    std::vector<TParsedIOEvent> parsedEvents;
    parsedEvents.reserve(ioEvents.size());
    std::transform(ioEvents.begin(), ioEvents.end(), std::back_inserter(parsedEvents), ParseEvent);
    return parsedEvents;
}

////////////////////////////////////////////////////////////////////////////////

class TRawEventSink
{
public:
    DEFINE_BYVAL_RW_PROPERTY(bool, Enabled);

    TRawEventSink(const TLogger& logger, bool enabled)
        : Enabled_(enabled)
        , Logger_(logger)
    { }

    void ConsumeBatch(const std::vector<TParsedIOEvent>& events)
    {
        if (!Enabled_) {
            return;
        }
        for (const auto& event : events) {
            DoConsume(event);
        }
    }

    DEFINE_SIGNAL(void(const TIOCounters&, const TIOTagList&), OnEventLogged);

protected:
    void DoConsume(const TParsedIOEvent& event)
    {
        auto addTag = [] (auto fluent, const auto& item) {
            fluent.Item(item.first).Value(item.second);
        };

        LogStructuredEventFluently(Logger_, ELogLevel::Info)
            .DoFor(event.AggregatingTags.Tags, addTag)
            .DoFor(event.NonAggregatingTags.Tags, addTag)
            .Item("bytes").Value(event.Counters.Bytes)
            .Item("io_requests").Value(event.Counters.IORequests);

        if (!OnEventLogged_.Empty()) {
            TIOTagList tags = event.AggregatingTags.Tags;
            tags.insert(
                tags.end(),
                event.NonAggregatingTags.Tags.begin(),
                event.NonAggregatingTags.Tags.end());
            OnEventLogged_.Fire(event.Counters, tags);
        }
    }

private:
    const TLogger& Logger_;
};

////////////////////////////////////////////////////////////////////////////////

class TAggregateEventSink
{
public:
    DEFINE_BYVAL_RW_PROPERTY(int, SizeLimit);
    DEFINE_BYVAL_RW_PROPERTY(TDuration, Period);

    TAggregateEventSink(
        const TLogger& logger,
        bool enabled,
        int sizeLimit,
        TDuration period,
        std::function<TAggregateTagsKey(const TParsedIOEvent&)> tagFilter,
        NProfiling::TProfiler profiler)
        : SizeLimit_(sizeLimit)
        , Period_(period)
        , Logger_(logger)
        , TagFilter_(std::move(tagFilter))
        , AggregateEventsWritten_(profiler.Counter("/aggregate_events_written"))
        , AggregateEventsDropped_(profiler.Counter("/aggregate_events_dropped"))
        , Enabled_(enabled)
    {
        ResetTimerWithSplay();
    }

    DEFINE_SIGNAL(void(const TIOCounters&, const TIOTagList&), OnEventLogged);

    void SetEnabled(bool value)
    {
        if (value == Enabled_) {
            return;
        }
        Enabled_ = value;
        if (value) {
            ResetTimerWithSplay();
        } else {
            FlushEvents();
        }
    }

    void ConsumeBatch(const std::vector<TParsedIOEvent>& events)
    {
        if (!Enabled_) {
            return;
        }
        for (const auto& event : events) {
            DoConsume(event);
        }
    }

    void TryFlush()
    {
        if (!Enabled_) {
            return;
        }
        auto now = Now();
        if (now > LastFlushed_ + Period_) {
            FlushEvents();
            LastFlushed_ = now;
        }
    }

protected:
    void DoConsume(const TParsedIOEvent& event)
    {
        auto key = TagFilter_(event);
        auto iterator = AggregateEvents_.find(key);
        if (iterator == AggregateEvents_.end()) {
            if (ssize(AggregateEvents_) >= SizeLimit_) {
                AggregateEventsDropped_.Increment();
                return;
            }
            iterator = AggregateEvents_.emplace(std::move(key), TIOCounters{}).first;
        }
        iterator->second.MergeFrom(event.Counters);
    }

private:
    const TLogger& Logger_;
    std::function<TAggregateTagsKey(const TParsedIOEvent&)> TagFilter_;
    THashMap<TAggregateTagsKey, TIOCounters> AggregateEvents_;
    TInstant LastFlushed_;
    NProfiling::TCounter AggregateEventsWritten_;
    NProfiling::TCounter AggregateEventsDropped_;
    bool Enabled_;

    void ResetTimerWithSplay()
    {
        TDuration splay = Period_.MicroSeconds() == 0
            ? TDuration::Zero()
            : TDuration::MicroSeconds(RandomNumber<ui64>(Period_.MicroSeconds()));
        LastFlushed_ = Now() - splay;
    }

    void FlushEvents()
    {
        for (const auto& event : AggregateEvents_) {
            // NB. We cannot do structure binding here, because key must be captured into lambda,
            // but bindings cannot be captured.
            const auto& key = event.first;
            const auto& counters = event.second;

            auto addTag = [] (auto fluent, const auto& item) {
                fluent
                    .Item(item.first).Value(item.second);
            };

            LogStructuredEventFluently(Logger_, ELogLevel::Info)
                .DoFor(key.InlineTags.Tags, addTag)
                .DoIf(key.NestedTags.has_value(), [&] (auto fluent) {
                    fluent
                        .Item("tags")
                        .BeginMap()
                        .DoFor(key.NestedTags->Tags, addTag)
                        .EndMap();
                })
                .Item("bytes").Value(counters.Bytes)
                .Item("io_requests").Value(counters.IORequests);

            if (!OnEventLogged_.Empty()) {
                auto allTags = key.InlineTags.Tags;
                if (key.NestedTags) {
                    allTags.insert(allTags.end(), key.NestedTags->Tags.begin(), key.NestedTags->Tags.end());
                }
                OnEventLogged_.Fire(counters, allTags);
            }
        }

        AggregateEventsWritten_.Increment(ssize(AggregateEvents_));

        AggregateEvents_.clear();
    }
};

////////////////////////////////////////////////////////////////////////////////

class TIOTracker
    : public IIOTracker
{
public:
    explicit TIOTracker(TIOTrackerConfigPtr config)
        : Profiler_(NProfiling::TProfiler{"/io_tracker"})
        , ActionQueue_(New<TActionQueue>("IOTracker"))
        , Invoker_(ActionQueue_->GetInvoker())
        , PeriodQuant_(config->PeriodQuant)
        , PathAggregateTags_(config->PathAggregateTags)
        , RawSink_(StructuredIORawLogger, config->Enable && config->EnableRaw)
        , AggregateSink_(
            StructuredIOAggregateLogger,
            config->Enable && config->EnableAggr,
            config->AggregationSizeLimit,
            config->AggregationPeriod,
            /*tagFilter*/ [] (const auto& event) { return TAggregateTagsKey{ .InlineTags = event.AggregatingTags }; },
            Profiler_)
        , PathAggregateSink_(
            StructuredIOPathAggregateLogger,
            config->Enable && config->EnablePath,
            config->AggregationSizeLimit,
            config->AggregationPeriod,
            /*tagFilter*/ [this] (const auto& event) { return FilterPathAggregateTags(event); },
            Profiler_)
        , Config_(std::move(config))
        , EventsDropped_(Profiler_.Counter("/events_dropped"))
        , EventsProcessed_(Profiler_.Counter("/events_processed"))
    {
        // NB. Passing MakeStrong here causes a memory leak because of cyclic reference. But it doesn't bother
        // us since this class is used as a singleton and is not intended to be destroyed.
        Invoker_->Invoke(BIND(&TIOTracker::Run, MakeStrong(this)));
    }

    bool IsEnabled() const override
    {
        return GetConfig()->Enable;
    }

    TIOTrackerConfigPtr GetConfig() const override
    {
        return Config_.Acquire();
    }

    void SetConfig(TIOTrackerConfigPtr config) override
    {
        // NB. We rely on FIFO order in TActionQueue here. Otherwise, the most recent config can
        // be replaced by an older one.
        Invoker_->Invoke(BIND(&TIOTracker::UpdateConfig, MakeWeak(this), std::move(config)));
    }

    void Enqueue(TIOEvent ioEvent) override
    {
        if (!IsEnabled()) {
            return;
        }
        if (EventStackSize_.load() >= GetConfig()->QueueSizeLimit) {
            EventsDropped_.Increment();
            return;
        }
        EventStackSize_.fetch_add(1);
        EventStack_.Enqueue(std::move(ioEvent));
    }

    DECLARE_SIGNAL_OVERRIDE(void(const TIOCounters&, const TIOTagList&), OnRawEventLogged);
    DECLARE_SIGNAL_OVERRIDE(void(const TIOCounters&, const TIOTagList&), OnAggregateEventLogged);
    DECLARE_SIGNAL_OVERRIDE(void(const TIOCounters&, const TIOTagList&), OnPathAggregateEventLogged);

private:
    const NProfiling::TProfiler Profiler_;
    const TActionQueuePtr ActionQueue_;
    const IInvokerPtr Invoker_;

    TMpscStack<TIOEvent> EventStack_;
    std::atomic<int> EventStackSize_ = 0;
    TDuration PeriodQuant_;
    THashSet<TString> PathAggregateTags_;
    TRawEventSink RawSink_;
    TAggregateEventSink AggregateSink_;
    TAggregateEventSink PathAggregateSink_;
    TAtomicIntrusivePtr<TIOTrackerConfig> Config_;
    NProfiling::TCounter EventsDropped_;
    NProfiling::TCounter EventsProcessed_;

    void UpdateConfig(TIOTrackerConfigPtr config)
    {
        VERIFY_INVOKER_AFFINITY(Invoker_);

        PeriodQuant_ = config->PeriodQuant;
        PathAggregateTags_ = config->PathAggregateTags;

        RawSink_.SetEnabled(config->Enable && config->EnableRaw);

        AggregateSink_.SetEnabled(config->Enable);
        AggregateSink_.SetSizeLimit(config->AggregationSizeLimit);
        AggregateSink_.SetPeriod(config->AggregationPeriod);

        PathAggregateSink_.SetEnabled(config->Enable && config->EnablePath);
        PathAggregateSink_.SetSizeLimit(config->AggregationSizeLimit);
        PathAggregateSink_.SetPeriod(config->AggregationPeriod);

        Config_.Store(std::move(config));
    }

    TAggregateTagsKey FilterPathAggregateTags(const TParsedIOEvent& event)
    {
        VERIFY_INVOKER_AFFINITY(Invoker_);

        auto nestedTags = event.AggregatingTags.FilterTags([&] (const auto& tag) {
            return PathAggregateTags_.contains(tag.first);
        });
        TSortedIOTagList inlineTags;
        if (auto path = event.NonAggregatingTags.FindTag(PathTagName)) {
            inlineTags.Tags.emplace_back(PathTagName, *path);
        }
        return TAggregateTagsKey{
            .InlineTags = std::move(inlineTags),
            .NestedTags = std::move(nestedTags),
        };
    }

    void Run()
    {
        VERIFY_INVOKER_AFFINITY(Invoker_);

        TPeriodicYielder yielder(TDuration::MilliSeconds(50));
        while (true) {
            if (!GetConfig()->EnableEventDequeue) {
                TDelayedExecutor::WaitForDuration(PeriodQuant_);
                continue;
            }
            auto events = EventStack_.DequeueAll(/*reverse*/ true);
            if (events.empty()) {
                AggregateSink_.TryFlush();
                PathAggregateSink_.TryFlush();
                TDelayedExecutor::WaitForDuration(PeriodQuant_);
                continue;
            }
            EventStackSize_.fetch_sub(ssize(events));

            auto parsedEvents = ParseEvents(events);
            RawSink_.ConsumeBatch(parsedEvents);
            AggregateSink_.ConsumeBatch(parsedEvents);
            AggregateSink_.TryFlush();
            PathAggregateSink_.ConsumeBatch(parsedEvents);
            PathAggregateSink_.TryFlush();
            EventsProcessed_.Increment(ssize(events));

            yielder.TryYield();
        }
    }
};

DELEGATE_SIGNAL_WITH_RENAME(
    TIOTracker,
    void(const TIOCounters&, const TIOTagList&),
    OnRawEventLogged,
    RawSink_,
    OnEventLogged);
DELEGATE_SIGNAL_WITH_RENAME(
    TIOTracker,
    void(const TIOCounters&, const TIOTagList&),
    OnAggregateEventLogged,
    AggregateSink_,
    OnEventLogged);
DELEGATE_SIGNAL_WITH_RENAME(
    TIOTracker,
    void(const TIOCounters&, const TIOTagList&),
    OnPathAggregateEventLogged,
    PathAggregateSink_,
    OnEventLogged);

////////////////////////////////////////////////////////////////////////////////

void IIOTracker::Enqueue(TIOCounters counters, THashMap<TString, TString> tags)
{
    auto* traceContext = NTracing::TryGetCurrentTraceContext();
    Enqueue(TIOEvent{
        .Counters = std::move(counters),
        .Baggage = traceContext ? traceContext->GetBaggage() : TYsonString{},
        .LocalTags = std::move(tags)
    });
}

////////////////////////////////////////////////////////////////////////////////

IIOTrackerPtr CreateIOTracker(TIOTrackerConfigPtr config)
{
    return New<TIOTracker>(std::move(config));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NIO
