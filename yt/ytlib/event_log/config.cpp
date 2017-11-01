#include "config.h"

namespace NYT {
namespace NEventLog {

////////////////////////////////////////////////////////////////////////////////

TEventLogConfig::TEventLogConfig()
{
    RegisterParameter("path", Path)
        .Default();
    RegisterParameter("pending_rows_flush_period", PendingRowsFlushPeriod)
        .Default(TDuration::Seconds(1));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NEventLog
} // namespace NYT
