#include "provider_load.h"

namespace NYT::NYqlPlugin {

////////////////////////////////////////////////////////////////////////////////

void ExtSolomon(
    const NYql::TGatewaysConfig& /*gatewaysConfig*/,
    const NKikimr::NMiniKQL::IFunctionRegistry* /*funcRegistry*/,
    TVector<NYql::TDataProviderInitializer>& /*dataProvidersInit*/,
    const NYql::TFileStoragePtr& /*fileStorage*/)
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYqlPlugin
