#pragma once

#include <yql/essentials/core/yql_data_provider.h>
#include <yql/essentials/core/file_storage/file_storage.h>
#include <yql/essentials/minikql/mkql_function_registry.h>
#include <yql/essentials/providers/common/proto/gateways_config.pb.h>

#include <util/generic/ptr.h>
#include <util/generic/vector.h>


namespace NYT::NYqlPlugin {

void ExtProviderSpecific(
    const NYql::TGatewaysConfig& gatewaysConfig,
    const NKikimr::NMiniKQL::IFunctionRegistry* funcRegistry,
    TVector<NYql::TDataProviderInitializer>& dataProvidersInit,
    const NYql::TFileStoragePtr& fileStorage);

void ExtYtflow(
    const NYql::TGatewaysConfig& gatewaysConfig,
    const NKikimr::NMiniKQL::IFunctionRegistry* funcRegistry,
    TVector<NYql::TDataProviderInitializer>& dataProvidersInit,
    const NYql::TFileStoragePtr& fileStorage);

void ExtPq(
    const NYql::TGatewaysConfig& gatewaysConfig,
    const NKikimr::NMiniKQL::IFunctionRegistry* funcRegistry,
    TVector<NYql::TDataProviderInitializer>& dataProvidersInit,
    const NYql::TFileStoragePtr& fileStorage);

} // namespace NYT::NYqlPlugin
