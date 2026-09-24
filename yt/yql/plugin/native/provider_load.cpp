#include "provider_load.h"


namespace NYT::NYqlPlugin {

void ExtProviderSpecific(
    const NYql::TGatewaysConfig& gatewaysConfig,
    const NYql::TStaticGatewaysConfig& staticGatewaysConfig,
    const NKikimr::NMiniKQL::IFunctionRegistry* funcRegistry,
    TVector<NYql::TDataProviderInitializer>& dataProvidersInit,
    const NYql::TFileStoragePtr& fileStorage)
{
    ExtYtflow(gatewaysConfig, staticGatewaysConfig, funcRegistry, dataProvidersInit, fileStorage);
    ExtPq(gatewaysConfig, funcRegistry, dataProvidersInit, fileStorage);
    ExtSolomon(gatewaysConfig, funcRegistry, dataProvidersInit, fileStorage);
}

} // namespace NYT::NYqlPlugin
