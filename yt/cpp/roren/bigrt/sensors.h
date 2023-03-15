#pragma once

#include <yt/yt/library/profiling/sensor.h>
#include <yt/yt/library/profiling/tag.h>

#include <util/generic/string.h>
#include <util/ysaveload.h>

#include <vector>

namespace NRoren {

////////////////////////////////////////////////////////////////////////////////

struct TTaggedSensor
{
public:
    TString Name;
    std::vector<NYT::NProfiling::TTag> Tags;

    Y_SAVELOAD_DEFINE(Name, Tags);

public:
    TTaggedSensor() = default;
    explicit TTaggedSensor(TString name, std::vector<NYT::NProfiling::TTag> tags = {});
};

NYT::NProfiling::TTagList GetTagList(const std::vector<NYT::NProfiling::TTag>& tags);

////////////////////////////////////////////////////////////////////////////////

class TCounterUpdater {
public:
    explicit TCounterUpdater(NYT::NProfiling::TProfiler profiler)
        : Profiler(std::move(profiler))
    {
    }

    template <class TSensor, class TSensorValue>
    void Update(const TSensor& taggedSensor, TSensorValue sensorValue) {
        if (taggedSensor) {
            Profiler.WithTags(NYT::NProfiling::TTagSet{GetTagList(taggedSensor->Tags)})
                .Counter(taggedSensor->Name)
                .Increment(sensorValue);
        }
    }

private:
    NYT::NProfiling::TProfiler Profiler;
};

////////////////////////////////////////////////////////////////////////////////

}

