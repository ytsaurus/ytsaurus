#pragma once

#include "public.h"

#include <yt/yt/core/ytree/yson_serializable.h>

namespace NYT::NTracing {

////////////////////////////////////////////////////////////////////////////////

class TTracingConfig
    : public NYTree::TYsonSerializable
{
public:
    bool SendBaggage;

    TTracingConfig()
    {
        RegisterParameter("send_baggage", SendBaggage)
            .Default(true);
    }
};

DEFINE_REFCOUNTED_TYPE(TTracingConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTracing
