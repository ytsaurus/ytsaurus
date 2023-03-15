#include "sensors.h"

namespace NRoren {

////////////////////////////////////////////////////////////////////////////////

TTaggedSensor::TTaggedSensor(TString name, std::vector<NYT::NProfiling::TTag> tags)
    : Name(std::move(name))
    , Tags(std::move(tags))
{ }

NYT::NProfiling::TTagList GetTagList(const std::vector<NYT::NProfiling::TTag>& tags) {
    return NYT::NProfiling::TTagList(tags.begin(), tags.end());
}

////////////////////////////////////////////////////////////////////////////////

}

