#include "proto.h"

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

void TProtoSourceComputationParameters::Register(TRegistrar registrar)
{
    registrar.Parameter("data_column", &TThis::DataColumn)
        .Default("data");
}

void TDynamicProtoSourceComputationParameters::Register(TRegistrar /*registrar*/)
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
