#include "stdafx.h"
#include "attribute_provider.h"

#include <ytlib/misc/protobuf_helpers.h>

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

TAttributeFilter TAttributeFilter::All(EAttributeFilterMode::All, std::vector<Stroka>());
TAttributeFilter TAttributeFilter::None(EAttributeFilterMode::None, std::vector<Stroka>());

NProto::TAttributeFilter ToProto(const TAttributeFilter& filter)
{
    NProto::TAttributeFilter protoFilter;
    protoFilter.set_mode(filter.Mode);
    FOREACH (const auto& key, filter.Keys) {
        protoFilter.add_keys(key);
    }
    return protoFilter;
}

TAttributeFilter FromProto(const NProto::TAttributeFilter& protoFilter)
{
    return TAttributeFilter(
        EAttributeFilterMode(protoFilter.mode()),
        NYT::FromProto<Stroka>(protoFilter.keys()));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
