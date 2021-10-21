#pragma once

#include "public.h"

#include <yt/yt/core/ytree/public.h>
#include <yt/yt/core/ytree/yson_serializable.h>

#include <optional>

namespace NYT::NProfiling {

////////////////////////////////////////////////////////////////////////////////

class TProfileManagerConfig
    : public NYTree::TYsonSerializable
{
public:
    TDuration MaxKeepInterval;
    TDuration DequeuePeriod;
    TDuration SampleRateLimit;

    THashMap<TString, TString> GlobalTags;

    TProfileManagerConfig();
};

DEFINE_REFCOUNTED_TYPE(TProfileManagerConfig);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NProfiling

