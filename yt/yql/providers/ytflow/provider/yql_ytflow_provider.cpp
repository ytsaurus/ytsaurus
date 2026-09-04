#include "yql_ytflow_provider.h"

#include <library/cpp/random_provider/random_provider.h>

#include <yql/essentials/core/yql_execution.h>
#include <yql/essentials/core/yql_type_annotation.h>
#include <yql/essentials/minikql/mkql_function_registry.h>
#include <yql/essentials/providers/common/proto/gateways_config.pb.h>
#include <yql/essentials/providers/common/provider/yql_provider_names.h>

#include <yt/yql/providers/ytflow/expr_nodes/yql_ytflow_expr_nodes.h>


namespace NYql {

TDataProviderInitializer GetYtflowDataProviderInitializer(IYtflowGateway::TPtr gateway) {
    return [gateway] (
        const TString& /*userName*/,
        const TString& sessionId,
        const TGatewaysConfig* gatewaysConfig,
        const NKikimr::NMiniKQL::IFunctionRegistry* /*functionRegistry*/,
        TIntrusivePtr<IRandomProvider> /*randomProvider*/,
        TIntrusivePtr<TTypeAnnotationContext> typeCtx,
        const TOperationProgressWriter& /*progressWriter*/,
        const TYqlOperationOptions& /*operationOptions*/,
        THiddenQueryAborter /*hiddenAborter*/,
        const TQContext& /*qContext*/
    ) {
        auto ytflowState = MakeIntrusive<TYtflowState>();

        ytflowState->SessionId = sessionId;
        ytflowState->Types = typeCtx.Get();
        ytflowState->Gateway = gateway;
        ytflowState->Configuration = MakeIntrusive<TYtflowConfiguration>();

        if (gatewaysConfig) {
            ytflowState->Configuration->Init(gatewaysConfig->GetYtflow());
        }

        TDataProviderInfo info;

        info.Names.insert({TString{YtflowProviderName}});
        info.Source = CreateYtflowDataSource(ytflowState);
        info.Sink = CreateYtflowDataSink(ytflowState);

        info.OpenSession = [gateway] (
            const TString& sessionId, const TString& userName,
            const TOperationProgressWriter& progressWriter, const TYqlOperationOptions& operationOptions,
            TIntrusivePtr<IRandomProvider> randomProvider, TIntrusivePtr<ITimeProvider> timeProvider
        ) {
            Y_UNUSED(userName, randomProvider, timeProvider);

            gateway->OpenSession(IYtflowGateway::TOpenSessionOptions()
                .SessionId(sessionId)
                .OperationProgressWriter(progressWriter)
                .OperationOptions(operationOptions));

            return NThreading::MakeFuture();
        };

        info.CloseSessionAsync = [gateway](const TString& sessionId) {
            return gateway->CloseSession(IYtflowGateway::TCloseSessionOptions()
                .SessionId(sessionId));
        };

        return info;
    };
}

namespace {

using namespace NNodes;


struct TYtflowDataSourceFunctions {
    THashSet<TStringBuf> Names;

    TYtflowDataSourceFunctions()
    {
    }
};


struct TYtflowDataSinkFunctions {
    THashSet<TStringBuf> Names;

    TYtflowDataSinkFunctions()
    {
    }
};

} // namespace


const THashSet<TStringBuf>& YtflowDataSourceFunctions() {
    return Default<TYtflowDataSourceFunctions>().Names;
}


const THashSet<TStringBuf>& YtflowDataSinkFunctions() {
    return Default<TYtflowDataSinkFunctions>().Names;
}

} // namespace NYql
