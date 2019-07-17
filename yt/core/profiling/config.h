#pragma once

#include "public.h"

#include <yt/core/ytree/public.h>
#include <yt/core/ytree/yson_serializable.h>

namespace NYT::NProfiling {

////////////////////////////////////////////////////////////////////////////////

class TProfileManagerConfig
    : public NYTree::TYsonSerializable
{
public:
    THashMap<TString, TString> GlobalTags;

    TProfileManagerConfig()
    {
        RegisterParameter("global_tags", GlobalTags)
            .Default();
    }
};

DEFINE_REFCOUNTED_TYPE(TProfileManagerConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NProfiling

