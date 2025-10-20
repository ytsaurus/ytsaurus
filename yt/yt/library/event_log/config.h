#pragma once

#include "public.h"

#include <yt/yt/core/ytree/yson_struct.h>

#include <yt/yt/core/ypath/public.h>

namespace NYT::NEventLog {

////////////////////////////////////////////////////////////////////////////////

struct TEventLogManagerConfig
    : public virtual NYTree::TYsonStruct
{
    bool Enable;
    TDuration PendingRowsFlushPeriod;

    REGISTER_YSON_STRUCT(TEventLogManagerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TEventLogManagerConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NEventLog
