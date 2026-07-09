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
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
