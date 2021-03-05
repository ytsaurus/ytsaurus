#pragma once

#include "public.h"

#include <yt/yt/ytlib/table_client/config.h>

namespace NYT::NEventLog {

////////////////////////////////////////////////////////////////////////////////

class TEventLogManagerConfig
    : public NTableClient::TBufferedTableWriterConfig
{
public:
    bool Enable;
    NYPath::TYPath Path;
    TDuration PendingRowsFlushPeriod;

    TEventLogManagerConfig();
};

DEFINE_REFCOUNTED_TYPE(TEventLogManagerConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NEventLog
