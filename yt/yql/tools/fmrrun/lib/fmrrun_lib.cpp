#include "fmrrun_lib.h"

#include <contrib/ydb/library/yql/dq/opt/dq_opt_join_cbo_factory.h>

#include <yt/yql/providers/yt/fmr/fmr_tool_lib/yql_yt_fmr_initializer.h>
#include <yql/essentials/providers/common/provider/yql_provider_names.h>
#include <yt/yql/providers/yt/gateway/file/yql_yt_file.h>

namespace NYql {

TFmrRunTool::TFmrRunTool()
    : TYqlRunTool()
{
    GetRunOptions().SetSupportedGateways({TString{YtProviderName}, TString{NFmr::FastMapReduceGatewayName}});
    GetRunOptions().AddOptExtension([this](NLastGetopt::TOpts& opts) {
        opts.AddLongOption( "table-data-service-discovery-file-path", "Table data service discovery file path")
            .Required()
            .StoreResult(&TableDataServiceDiscoveryFilePath_);
    });
}

IOptimizerFactory::TPtr TFmrRunTool::CreateCboFactory() {
    return NYql::NDq::MakeCBOOptimizerFactory();
}

IYtGateway::TPtr TFmrRunTool::CreateYtGateway() {
    auto fileGateway = TYqlRunTool::CreateYtGateway();
    auto fmrServices = MakeIntrusive<NFmr::TFmrServices>();
    fmrServices->FunctionRegistry = GetFuncRegistry().Get();
    fmrServices->JobLauncher = MakeIntrusive<NFmr::TFmrUserJobLauncher>(false);
    fmrServices->TableDataServiceDiscoveryFilePath = TableDataServiceDiscoveryFilePath_;
    fmrServices->YtJobService = NFmr::MakeFileYtJobSerivce();
    fmrServices->YtCoordinatorService = NFmr::MakeFileYtCoordinatorService();
    auto [fmrGateway, worker] = NFmr::InitializeFmrGateway(fileGateway, fmrServices);
    FmrWorker_ = std::move(worker);
    return fmrGateway;
}

} // NYql
