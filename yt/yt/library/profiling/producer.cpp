#include "producer.h"

#include <util/system/compiler.h>

namespace NYT::NProfiling {

////////////////////////////////////////////////////////////////////////////////

void TSensorBuffer::PushTag(const TTag& tag)
{
    Tags_.push_back(tag);
}

void TSensorBuffer::PopTag()
{
    Tags_.pop_back();
}

void TSensorBuffer::AddGauge(const TString& name, double value)
{
    Gauges_.emplace_back(name, Tags_, value);
}

void TSensorBuffer::AddCounter(const TString& name, i64 value)
{
    Counters_.emplace_back(name, Tags_, value);
}

const std::vector<std::tuple<TString, TTagList, i64>>& TSensorBuffer::GetCounters() const
{
    return Counters_;
}

const std::vector<std::tuple<TString, TTagList, double>>& TSensorBuffer::GetGauges() const
{
    return Gauges_;
}

void TSensorBuffer::WriteTo(ISensorWriter* writer)
{
    for (const auto& [name, tags, value] : Counters_) {
        for (const auto& tag : tags) {
            writer->PushTag(tag);
        }

        writer->AddCounter(name, value);

        for (size_t i = 0; i < tags.size(); i++) {
            writer->PopTag();
        }
    }

    for (const auto& [name, tags, value] : Gauges_) {
        for (const auto& tag : tags) {
            writer->PushTag(tag);
        }

        writer->AddGauge(name, value);

        for (size_t i = 0; i < tags.size(); i++) {
            writer->PopTag();
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NProfiling
