#include <yql/essentials/utils/runnable.h>
#include <yt/yql/providers/yt/fmr/coordinator/interface/yql_yt_coordinator.h>
#include <yt/yql/providers/yt/fmr/tvm/interface/yql_yt_fmr_tvm_interface.h>

namespace NYql::NFmr {

using IFmrServer = IRunnable;

struct TFmrCoordinatorServerSettings {
    ui16 Port;
    TString Host = "localhost";
    std::vector<TTvmId> AllowedSourceTvmIds;
};

IFmrServer::TPtr MakeFmrCoordinatorServer(
    IFmrCoordinator::TPtr coordinator,
    const TFmrCoordinatorServerSettings& settings,
    IFmrTvmClient::TPtr tvmClient = nullptr
);

} // namespace NYql::NFmr
