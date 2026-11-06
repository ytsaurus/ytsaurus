#include "dynamic_counters.h"

#include <library/cpp/yt/memory/new.h>

#include <util/generic/algorithm.h>
#include <util/string/cast.h>

namespace NYql {
namespace {

class TCounterVisitor final : public NMonitoring::ICountableConsumer {
public:
    explicit TCounterVisitor(NYT::NProfiling::ISensorWriter* writer)
        : Writer_(writer)
    { }

    void OnGroupBegin(const TString& name, const TString& value, const NMonitoring::TDynamicCounters*) override {
        if (name) {
            PushTag(name, value);
        }
    }

    void OnGroupEnd(const TString& name, const TString&, const NMonitoring::TDynamicCounters*) override {
        if (name) {
            PopTag();
        }
    }

    void OnCounter(const TString& label, const TString& name, const NMonitoring::TCounterForPtr* counter) override {
        const bool named = label != "sensor";
        if (named) {
            PushTag("counter_label", label);
        }
        WriteValue("/" + name, counter->Val(), counter->ForDerivative());
        if (named) {
            PopTag();
        }
    }

    void OnHistogram(const TString& label, const TString& name,
        NMonitoring::IHistogramSnapshotPtr snapshot, bool derivative) override
    {
        const bool named = label != "sensor";
        if (named) {
            PushTag("counter_label", label);
        }
        for (ui32 i = 0; i < snapshot->Count(); ++i) {
            const auto bound = snapshot->UpperBound(i);
            PushTag("upper_bound", bound == NMonitoring::HISTOGRAM_INF_BOUND ? "+Inf" : ToString(bound));
            WriteValue("/" + name + "/bucket", snapshot->Value(i), derivative);
            PopTag();
        }
        if (named) {
            PopTag();
        }
    }

private:
    void WriteValue(TStringBuf name, i64 value, bool derivative) {
        if (derivative) {
            Writer_->AddCounter(name, value);
        } else {
            Writer_->AddGauge(name, value);
        }
    }

    void PushTag(const TString& name, const TString& value) {
        auto key = name;
        for (ui32 suffix = 2; Find(TagKeys_, key) != TagKeys_.end(); ++suffix) {
            key = name + "_" + ToString(suffix);
        }
        TagKeys_.push_back(key);
        Writer_->PushTag({std::string(key), std::string(value)});
    }

    void PopTag() {
        Writer_->PopTag();
        TagKeys_.pop_back();
    }

    NYT::NProfiling::ISensorWriter* const Writer_;
    std::vector<TString> TagKeys_;
};

class TDynamicCountersProducer : public NYT::NProfiling::ISensorProducer {
public:
    explicit TDynamicCountersProducer(NMonitoring::TDynamicCounterPtr counters)
        : Counters_(std::move(counters))
    { }

    void CollectSensors(NYT::NProfiling::ISensorWriter* writer) override {
        TCounterVisitor visitor(writer);
        // Accept traverses snapshots of the group maps; counter reads are atomic.
        Counters_->Accept({}, {}, visitor);
    }

private:
    const NMonitoring::TDynamicCounterPtr Counters_;
};

} // namespace

NYT::NProfiling::ISensorProducerPtr CreateDynamicCountersProducer(NMonitoring::TDynamicCounterPtr counters) {
    return NYT::New<TDynamicCountersProducer>(std::move(counters));
}

} // namespace NYql
