#include "stdafx.h"
#include "attribute_provider.h"

#include <core/misc/protobuf_helpers.h>

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

TAttributeFilter TAttributeFilter::All(EAttributeFilterMode::All, std::vector<Stroka>());
TAttributeFilter TAttributeFilter::None(EAttributeFilterMode::None, std::vector<Stroka>());

TAttributeFilter::TAttributeFilter()
    : Mode(EAttributeFilterMode::None)
{ }

TAttributeFilter::TAttributeFilter(
    EAttributeFilterMode mode,
    const std::vector<Stroka>& keys)
    : Mode(mode)
    , Keys(keys)
{ }

TAttributeFilter::TAttributeFilter(EAttributeFilterMode mode)
    : Mode(mode)
{ }

void ToProto(NProto::TAttributeFilter* protoFilter, const TAttributeFilter& filter)
{
    protoFilter->set_mode(static_cast<int>(filter.Mode));
    for (const auto& key : filter.Keys) {
        protoFilter->add_keys(key);
    }
}

void FromProto(TAttributeFilter* filter, const NProto::TAttributeFilter& protoFilter)
{
    *filter = TAttributeFilter(
        EAttributeFilterMode(protoFilter.mode()),
        NYT::FromProto<Stroka>(protoFilter.keys()));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
