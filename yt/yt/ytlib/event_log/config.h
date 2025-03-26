#pragma once

#include "public.h"

#include <yt/yt/ytlib/table_client/config.h>

namespace NYT::NEventLog {

////////////////////////////////////////////////////////////////////////////////

struct TEventLogManagerConfig
    : public NTableClient::TBufferedTableWriterConfig
{
    bool Enable;
    NYPath::TYPath Path;
    TDuration PendingRowsFlushPeriod;

    REGISTER_YSON_STRUCT(TEventLogManagerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TEventLogManagerConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NEventLog
