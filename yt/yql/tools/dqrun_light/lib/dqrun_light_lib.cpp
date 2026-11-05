#include "dqrun_light_lib.h"

#include <yql/essentials/providers/common/provider/yql_provider_names.h>

namespace NYql {

namespace {

const std::initializer_list<TString> SUPPORTED_GATEWAYS = {
    TString{DqProviderName},
    TString{YtProviderName},
    TString{"hybrid"},
};

} // namespace

TDqRunLightTool::TDqRunLightTool()
    : TDqRunToolBase("dqrun_light")
{
    GetRunOptions().SetSupportedGateways(SUPPORTED_GATEWAYS);
    GetRunOptions().GatewayTypes.insert(SUPPORTED_GATEWAYS);
}

} // NYql
