#pragma once

#include "public.h"

#include <yt/core/ytree/yson_serializable.h>

namespace NYT {
namespace NNet {

////////////////////////////////////////////////////////////////////////////////

class TDialerConfig
    : public NYTree::TYsonSerializable
{
public:
    int Priority;
    bool EnableNoDelay;
    bool EnableAggressiveReconnect;

    TDuration MinRto;
    TDuration MaxRto;
    double RtoScale;

    TDialerConfig()
    {
        RegisterParameter("priority", Priority)
            .InRange(0, 6)
            .Default(0);
        RegisterParameter("enable_no_delay", EnableNoDelay)
            .Default(true);
        RegisterParameter("enable_aggressive_reconnect", EnableAggressiveReconnect)
            .Default(false);
        RegisterParameter("min_rto", MinRto)
            .Default(TDuration::MilliSeconds(100));
        RegisterParameter("max_rto", MaxRto)
            .Default(TDuration::Seconds(30));
        RegisterParameter("rto_scale", RtoScale)
            .GreaterThan(0.0)
            .Default(2.0);
        
        RegisterValidator([&] () {
            if (MaxRto < MinRto) {
                THROW_ERROR_EXCEPTION("\"max_rto\" should be greater than or equal to \"min_rto\"");
            }
        });
    }
};

DEFINE_REFCOUNTED_TYPE(TDialerConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NNet
} // namespace NYT
