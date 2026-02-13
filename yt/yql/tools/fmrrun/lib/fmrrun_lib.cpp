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
            .Optional()
            .StoreResult(&TableDataServiceDiscoveryFilePath_);
        opts.AddLongOption("fmr-coordinator-server-url", "Fast map reduce coordinator server url")
            .Optional()
            .StoreResult(&FmrCoordinatorServerUrl_);
        opts.AddLongOption("disable-local-fmr-worker", "Disable local fast map reduce worker")
            .Optional()
            .NoArgument()
            .SetFlag(&DisableLocalFmrWorker_);
        opts.AddLongOption( "fmrjob-bin", "Path to fmrjob binary")
            .Optional()
            .StoreResult(&FmrJobBin_);
    });
}

IOptimizerFactory::TPtr TFmrRunTool::CreateCboFactory() {
    return NYql::NDq::MakeCBOOptimizerFactory();
}

IYtGateway::TPtr TFmrRunTool::CreateYtGateway() {
    auto fileGateway = TYqlRunTool::CreateYtGateway();
    auto fmrServices = MakeIntrusive<NFmr::TFmrServices>();
    fmrServices->FunctionRegistry = GetFuncRegistry().Get();
    fmrServices->JobLauncher = MakeIntrusive<NFmr::TFmrUserJobLauncher>(NFmr::TFmrUserJobLauncherOptions{
        .RunInSeparateProcess = true,
        .FmrJobBinaryPath = FmrJobBin_,
        .TableDataServiceDiscoveryFilePath = TableDataServiceDiscoveryFilePath_,
        .GatewayType = "file"
    });
    fmrServices->TableDataServiceDiscoveryFilePath = TableDataServiceDiscoveryFilePath_;
    fmrServices->YtJobService = NFmr::MakeFileYtJobService();
    fmrServices->YtCoordinatorService = NFmr::MakeFileYtCoordinatorService();
    fmrServices->CoordinatorServerUrl = FmrCoordinatorServerUrl_;
    fmrServices->DisableLocalFmrWorker = DisableLocalFmrWorker_;
    fmrServices->NeedToTransformTmpTablePaths = false;

    fmrServices->FileStorage = GetFileStorage();
    if (!fmrServices->DisableLocalFmrWorker) {
        auto jobPreparer = NFmr::MakeFmrJobPreparer(GetFileStorage(), TableDataServiceDiscoveryFilePath_);
        fmrServices->JobPreparer = jobPreparer;
    }

    auto [fmrGateway, worker] = NFmr::InitializeFmrGateway(fileGateway, fmrServices);
    FmrWorker_ = std::move(worker);
    return fmrGateway;
}

} // NYql
