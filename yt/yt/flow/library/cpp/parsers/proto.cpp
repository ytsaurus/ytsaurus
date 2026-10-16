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

void TProtoTransformSourceComputationParameters::Register(TRegistrar registrar)
{
    registrar.Parameter("data_column", &TThis::DataColumn)
        .Default("data");
}

void TDynamicProtoTransformSourceComputationParameters::Register(TRegistrar /*registrar*/)
{ }

////////////////////////////////////////////////////////////////////////////////

void TProtoParsingProcessFunctionParameters::Register(TRegistrar registrar)
{
    registrar.Parameter("data_column", &TThis::DataColumn)
        .Default("data");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
