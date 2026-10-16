#include "stream_statistics.h"

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

void TStreamSpeedStatistics::Register(TRegistrar registrar)
{
    registrar.Parameter("processed_messages_per_second", &TThis::ProcessedMessagesPerSecond)
        .Default(0.0);
    registrar.Parameter("processed_bytes_per_second", &TThis::ProcessedBytesPerSecond)
        .Default(0.0);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
