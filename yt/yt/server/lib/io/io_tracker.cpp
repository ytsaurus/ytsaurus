#include "io_tracker.h"

#include "private.h"
#include "config.h"

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/delayed_executor.h>
#include <yt/yt/core/concurrency/periodic_yielder.h>

#include <yt/yt/core/misc/atomic_object.h>
#include <yt/yt/core/misc/mpsc_stack.h>

#include <yt/yt/core/logging/fluent_log.h>

#include <yt/yt/core/yson/pull_parser.h>

namespace NYT::NIO {

using namespace NYTree;
using namespace NConcurrency;
using namespace NLogging;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

void TIOCounters::MergeFrom(TIOCounters other)
{
    ByteCount += other.ByteCount;
    IOCount += other.IOCount;
}

////////////////////////////////////////////////////////////////////////////////

struct TSortedIOTagList
{
    TIOTagList Tags;

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

    explicit TRawEventSink(bool enabled)
        : Enabled_(enabled)
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

    DEFINE_SIGNAL(void(const TIOCounters&, const TIOTagList&), OnRawEventLogged);

protected:
    void DoConsume(const TParsedIOEvent& event)
    {
        auto addTag = [] (auto fluent, const auto& item) {
            fluent.Item(item.first).Value(item.second);
        };

        LogStructuredEventFluently(StructuredIORawLogger, ELogLevel::Info)
            .DoFor(event.AggregatingTags.Tags, addTag)
            .DoFor(event.NonAggregatingTags.Tags, addTag)
            .Item("byte_count").Value(event.Counters.ByteCount)
            .Item("io_count").Value(event.Counters.IOCount);

        if (!OnRawEventLogged_.Empty()) {
            TIOTagList tags = event.AggregatingTags.Tags;
            tags.insert(
                tags.end(),
                event.NonAggregatingTags.Tags.begin(),
                event.NonAggregatingTags.Tags.end());
            OnRawEventLogged_.Fire(event.Counters, tags);
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

class TAggregateEventSink
{
public:
    DEFINE_BYVAL_RW_PROPERTY(int, SizeLimit);
    DEFINE_BYVAL_RW_PROPERTY(TDuration, Period);

    TAggregateEventSink(
        bool enabled,
        int sizeLimit,
        TDuration period,
        NProfiling::TProfiler profiler)
        : SizeLimit_(sizeLimit)
        , Period_(period)
        , AggregateEventsWritten_(profiler.Counter("/aggregate_events_written"))
        , AggregateEventsDropped_(profiler.Counter("/aggregate_events_dropped"))
        , Enabled_(enabled)
    { }

    DEFINE_SIGNAL(void(const TIOCounters&, const TIOTagList&), OnAggregateEventLogged);

    void SetEnabled(bool value)
    {
        if (value == Enabled_) {
            return;
        }
        Enabled_ = value;
        if (value) {
            LastFlushed_ = Now();
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
        auto iterator = AggregateEvents_.find(event.AggregatingTags);
        if (iterator == AggregateEvents_.end()) {
            if (ssize(AggregateEvents_) >= SizeLimit_) {
                AggregateEventsDropped_.Increment();
                return;
            }
            iterator = AggregateEvents_.emplace(event.AggregatingTags, TIOCounters{}).first;
        }
        iterator->second.MergeFrom(event.Counters);
    }

private:
    THashMap<TSortedIOTagList, TIOCounters> AggregateEvents_;
    TInstant LastFlushed_ = Now();
    NProfiling::TCounter AggregateEventsWritten_;
    NProfiling::TCounter AggregateEventsDropped_;
    bool Enabled_;

    void FlushEvents()
    {
        for (const auto& [tags, counters] : AggregateEvents_) {
            auto addTag = [] (auto fluent, const auto& item) {
                fluent
                    .Item(item.first).Value(item.second);
            };

            LogStructuredEventFluently(StructuredIORawLogger, ELogLevel::Info)
                .DoFor(tags.Tags, addTag)
                .Item("byte_count").Value(counters.ByteCount)
                .Item("io_count").Value(counters.IOCount);

            OnAggregateEventLogged_.Fire(counters, tags.Tags);
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
        , RawSink_(config->Enable && config->EnableRaw)
        , AggregateSink_(config->Enable, config->AggregationSizeLimit, config->AggregationPeriod, Profiler_)
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
        return Config_.Load()->Enable;
    }

    void SetConfig(TIOTrackerConfigPtr config) override
    {
        // NB. We rely on FIFO order in TActionQueue here. Otherwise, the most recent config can
        // be replaced by an older one.
        Invoker_->Invoke(BIND(&TIOTracker::UpdateConfig, MakeWeak(this), std::move(config)));
    }

    void Enqueue(TIOEvent ioEvent) override
    {
        if (EventStackSize_.load() >= Config_.Load()->QueueSizeLimit) {
            EventsDropped_.Increment();
            return;
        }
        EventStackSize_.fetch_add(1);
        EventStack_.Enqueue(std::move(ioEvent));
    }

    DECLARE_SIGNAL_OVERRIDE(void(const TIOCounters&, const TIOTagList&), OnRawEventLogged);
    DECLARE_SIGNAL_OVERRIDE(void(const TIOCounters&, const TIOTagList&), OnAggregateEventLogged);

private:
    NProfiling::TProfiler Profiler_;
    TActionQueuePtr ActionQueue_;
    IInvokerPtr Invoker_;
    TMpscStack<TIOEvent> EventStack_;
    std::atomic<int> EventStackSize_ = 0;
    TDuration PeriodQuant_;
    TRawEventSink RawSink_;
    TAggregateEventSink AggregateSink_;
    TAtomicObject<TIOTrackerConfigPtr> Config_;
    NProfiling::TCounter EventsDropped_;
    NProfiling::TCounter EventsProcessed_;

    void UpdateConfig(TIOTrackerConfigPtr config)
    {
        VERIFY_INVOKER_AFFINITY(Invoker_);

        PeriodQuant_ = config->PeriodQuant;
        RawSink_.SetEnabled(config->Enable && config->EnableRaw);
        AggregateSink_.SetEnabled(config->Enable);
        AggregateSink_.SetSizeLimit(config->AggregationSizeLimit);
        AggregateSink_.SetPeriod(config->AggregationPeriod);

        Config_.Store(std::move(config));
    }

    void Run()
    {
        VERIFY_INVOKER_AFFINITY(Invoker_);

        TPeriodicYielder yielder(TDuration::MilliSeconds(50));
        while (true) {
            auto events = EventStack_.DequeueAll(/*reverse*/ true);
            if (events.empty()) {
                AggregateSink_.TryFlush();
                TDelayedExecutor::WaitForDuration(PeriodQuant_);
                continue;
            }
            EventStackSize_.fetch_sub(ssize(events));

            auto parsedEvents = ParseEvents(events);
            RawSink_.ConsumeBatch(parsedEvents);
            AggregateSink_.ConsumeBatch(parsedEvents);
            AggregateSink_.TryFlush();
            EventsProcessed_.Increment(ssize(events));

            yielder.TryYield();
        }
    }
};

DELEGATE_SIGNAL(TIOTracker, void(const TIOCounters&, const TIOTagList&), OnRawEventLogged, RawSink_);
DELEGATE_SIGNAL(TIOTracker, void(const TIOCounters&, const TIOTagList&), OnAggregateEventLogged, AggregateSink_);

////////////////////////////////////////////////////////////////////////////////

IIOTrackerPtr CreateIOTracker(TIOTrackerConfigPtr config)
{
    return New<TIOTracker>(std::move(config));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NIO
