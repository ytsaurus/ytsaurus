#include "provider_load.h"

#include <yt/yql/providers/ytflow/gateway/yql_ytflow.h>
#include <yt/yql/providers/ytflow/provider/yql_ytflow_provider.h>


namespace NYT::NYqlPlugin {

void ExtYtflow(
    const NYql::TGatewaysConfig& gatewaysConfig,
    const NKikimr::NMiniKQL::IFunctionRegistry* funcRegistry,
    TVector<NYql::TDataProviderInitializer>& dataProvidersInit,
    const NYql::TFileStoragePtr& fileStorage)
{
    if (gatewaysConfig.HasYtflow()) {
        auto ytflowGateway = NYql::CreateYtflowGateway(NYql::TYtflowServices {
            .FunctionRegistry = funcRegistry,
            .FileStorage = fileStorage,
            .Config = std::make_shared<NYql::TYtflowGatewayConfig>(
                gatewaysConfig.GetYtflow())
        });

        dataProvidersInit.push_back(
            NYql::GetYtflowDataProviderInitializer(ytflowGateway));
    }
}

} // namespace NYT::NYqlPlugin
