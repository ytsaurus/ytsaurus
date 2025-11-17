#include "provider_load.h"


namespace NYT::NYqlPlugin {

void ExtProviderSpecific(
    const NYql::TGatewaysConfig& gatewaysConfig,
    const NKikimr::NMiniKQL::IFunctionRegistry* funcRegistry,
    TVector<NYql::TDataProviderInitializer>& dataProvidersInit,
    const NYql::TFileStoragePtr& fileStorage)
{
    ExtYtflow(gatewaysConfig, funcRegistry, dataProvidersInit, fileStorage);
    ExtPq(gatewaysConfig, funcRegistry, dataProvidersInit, fileStorage);
}

} // namespace NYT::NYqlPlugin
