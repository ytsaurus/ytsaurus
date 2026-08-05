#include "spec.h"

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

void TDynamicRandomSourceParameters::Register(TRegistrar registrar)
{
    registrar.Parameter("partition_count", &TThis::PartitionCount)
        .Default(3);
    registrar.Parameter("partition_message_count", &TThis::PartitionMessageCount)
        .Default(std::nullopt);
    registrar.Parameter("message_size_mean", &TThis::MessageSizeMean)
        .Default(1_KB);
    registrar.Parameter("message_count_mean", &TThis::MessageCountMean)
        .Default(1000000);
    registrar.Parameter("message_key_range", &TThis::MessageKeyRange)
        .Default(1024);
    registrar.Parameter("reported_backlog_bytes_per_second", &TThis::ReportedBacklogBytesPerSecond)
        .Default();
    registrar.Parameter("reported_backlog_messages_per_second", &TThis::ReportedBacklogMessagesPerSecond)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
