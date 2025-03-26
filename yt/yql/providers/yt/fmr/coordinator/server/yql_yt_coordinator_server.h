#include <yql/essentials/utils/runnable.h>
#include <yt/yql/providers/yt/fmr/coordinator/interface/yql_yt_coordinator.h>

namespace NYql::NFmr {

using IFmrServer = IRunnable;

struct TFmrCoordinatorServerSettings {
    ui16 Port;
    TString Host = "localhost";
};

IFmrServer::TPtr MakeFmrCoordinatorServer(IFmrCoordinator::TPtr coordinator, const TFmrCoordinatorServerSettings& settings);

} // namespace NYql::NFmr
