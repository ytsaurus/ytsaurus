#pragma once

#include "public.h"

#include <yt/ytlib/table_client/config.h>

namespace NYT::NEventLog {

////////////////////////////////////////////////////////////////////////////////

class TEventLogConfig
    : public NTableClient::TBufferedTableWriterConfig
{
public:
    NYPath::TYPath Path;
    TDuration PendingRowsFlushPeriod;

    TEventLogConfig();
};

DEFINE_REFCOUNTED_TYPE(TEventLogConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NEventLog
