#pragma once

#include <yt/yt/core/compression/public.h>

#include <yt/yt/core/yson/string.h>

namespace NYT::NObjectServer {

////////////////////////////////////////////////////////////////////////////////

NYson::TYsonString CreateFeatureRegistryYson(
    const std::optional<THashSet<NCompression::ECodec>>& configuredDeprecatedCodecIds);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NObjectServer

