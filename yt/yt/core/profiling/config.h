#pragma once

#include "public.h"

#include <yt/yt/core/ytree/yson_struct.h>

#include <optional>

namespace NYT::NProfiling {

////////////////////////////////////////////////////////////////////////////////

class TProfileManagerConfig
    : public NYTree::TYsonStruct
{
public:
    TDuration MaxKeepInterval;
    TDuration DequeuePeriod;
    TDuration SampleRateLimit;

    THashMap<TString, TString> GlobalTags;

    REGISTER_YSON_STRUCT(TProfileManagerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TProfileManagerConfig);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NProfiling

