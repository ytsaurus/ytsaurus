#include "config.h"

namespace NYT::NTimestampServer {

////////////////////////////////////////////////////////////////////////////////

void TTimestampManagerConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("calibration_period", &TThis::CalibrationPeriod)
        .Default(TDuration::MilliSeconds(100));
    registrar.Parameter("timestamp_preallocation_interval", &TThis::TimestampPreallocationInterval)
        .Alias("commit_advance")
        .Default(TDuration::Seconds(5));
    registrar.Parameter("max_timestamps_per_request", &TThis::MaxTimestampsPerRequest)
        .GreaterThan(0)
        .Default(1000000);
    registrar.Parameter("request_backoff_time", &TThis::RequestBackoffTime)
        .Default(TDuration::MilliSeconds(100));
    registrar.Parameter("embed_cell_tag", &TThis::EmbedCellTag)
        .Default(false);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTimestampServer
