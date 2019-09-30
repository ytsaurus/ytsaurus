#include "config.h"

namespace NYT::NEventLog {

////////////////////////////////////////////////////////////////////////////////

TEvenTLogManagerConfig::TEvenTLogManagerConfig()
{
    RegisterParameter("path", Path)
        .Default();
    RegisterParameter("pending_rows_flush_period", PendingRowsFlushPeriod)
        .Default(TDuration::Seconds(1));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NEventLog
