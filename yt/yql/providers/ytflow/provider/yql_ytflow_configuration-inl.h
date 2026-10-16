#pragma once

#include "yql_ytflow_configuration.h"


namespace NYql {

template <class TProtoConfig>
void TYtflowConfiguration::Init(const TProtoConfig& config)
{
    this->Dispatch(config.GetDefaultSettings());
}

} // namespace NYql
