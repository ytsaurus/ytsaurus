#pragma once

#include "public.h"

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NTimestampServer {

////////////////////////////////////////////////////////////////////////////////

class TTimestampManagerConfig
    : public NYTree::TYsonStruct
{
public:
    TDuration CalibrationPeriod;
    TDuration TimestampPreallocationInterval;
    int MaxTimestampsPerRequest;
    TDuration RequestBackoffTime;
    bool EmbedCellTag;

    REGISTER_YSON_STRUCT(TTimestampManagerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TTimestampManagerConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTimestampServer
