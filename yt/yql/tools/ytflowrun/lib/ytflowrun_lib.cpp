#include "ytflowrun_lib.h"

#include "provider_load.h"

#include <yt/yql/providers/ytflow/gateway/yql_ytflow.h>
#include <yt/yql/providers/ytflow/provider/yql_ytflow_provider.h>

#include <yql/essentials/providers/common/provider/yql_provider_names.h>

namespace NYql {

TYtflowRunTool::TYtflowRunTool(TString name)
    : TYtRunTool(std::move(name))
{
    GetRunOptions().SetSupportedGateways({TString{YtflowProviderName}});
    GetRunOptions().GatewayTypes.emplace(YtflowProviderName);

    AddProviderFactory([this]() -> NYql::TDataProviderInitializer {
        if (GetRunOptions().GatewayTypes.contains(YtflowProviderName) && GetRunOptions().GatewaysConfig->HasYtflow()) {
            return GetYtflowDataProviderInitializer(CreateYtflowGateway());
        }
        return {};
    });

    ExtProviderSpecific(this);
}

IYtflowGateway::TPtr TYtflowRunTool::CreateYtflowGateway() {
    return ::NYql::CreateYtflowGateway(NYql::TYtflowServices{
        .FunctionRegistry = GetFuncRegistry().Get(),
        .FileStorage = GetFileStorage(),
        .Config = std::make_shared<
            NYql::TYtflowGatewayConfig>(GetRunOptions().GatewaysConfig->GetYtflow()),
    });
}

int TYtflowRunTool::DoMain(int argc, const char *argv[]) {
    return TYtRunTool::DoMain(argc, argv);
}

TProgram::TStatus TYtflowRunTool::DoRunProgram(TProgramPtr program) {
    return TYtRunTool::DoRunProgram(program);
}

} // NYql
