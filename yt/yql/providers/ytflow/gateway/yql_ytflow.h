#pragma once

#include <yql/essentials/core/file_storage/file_storage.h>
#include <yql/essentials/minikql/mkql_function_registry.h>

#include <yt/yql/providers/ytflow/provider/yql_ytflow_gateway.h>

#include <memory>


namespace NYql {

class TYtflowGatewayConfig;
using TYtflowGatewayConfigPtr = std::shared_ptr<TYtflowGatewayConfig>;

struct TYtflowServices
{
    const NKikimr::NMiniKQL::IFunctionRegistry* FunctionRegistry = nullptr;

    TFileStoragePtr FileStorage;
    TYtflowGatewayConfigPtr Config;
};

IYtflowGateway::TPtr CreateYtflowGateway(const TYtflowServices& services);

} // namespace NYql
