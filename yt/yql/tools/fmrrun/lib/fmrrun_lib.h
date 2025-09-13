#pragma once

#include <yql/tools/yqlrun/lib/yqlrun_lib.h>
#include <yt/yql/providers/yt/fmr/worker/impl/yql_yt_worker_impl.h>

namespace NYql {

class TFmrRunTool: public TYqlRunTool {
public:
    TFmrRunTool();
    ~TFmrRunTool() = default;

protected:
    virtual IYtGateway::TPtr CreateYtGateway() override;

    virtual NYql::IOptimizerFactory::TPtr CreateCboFactory() override;
protected:
    TString TableDataServiceDiscoveryFilePath_;
    TString FmrCoordinatorServerUrl_;
    bool DisableLocalFmrWorker_ = false;
    TString FmrJobBin_;

private:
    NFmr::IFmrWorker::TPtr FmrWorker_;
};

} // NYql
