#include "config.h"

namespace NYT::NEventLog {

////////////////////////////////////////////////////////////////////////////////

void TEventLogManagerConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("enable", &TThis::Enable)
        .Default(true);
    registrar.Parameter("path", &TThis::Path)
        .Default();
    registrar.Parameter("pending_rows_flush_period", &TThis::PendingRowsFlushPeriod)
        .Default(TDuration::Seconds(1));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NEventLog
