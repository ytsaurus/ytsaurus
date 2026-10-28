#include "coordination_helper.h"

namespace NYql {

ICoordinationHelper::TPtr CreateUploadCoordinationHelper(const TString& /*hostName*/)
{
    return nullptr;
}

ICoordinationHelper::TPtr CreateCoordiantionHelper(
    const NProto::TDqConfig::TYtCoordinator& /*config*/,
    const NProto::TDqConfig::TScheduler& /*schedulerConfig*/,
    const TString& /*role*/,
    ui16 /*interconnectPort*/,
    const TString& /*host*/,
    const TString& /*ip*/,
    TGlobalWorkerManagerActorIdOptions /*actorIdOptions*/)
{
    return nullptr;
}

} // namespace NYql
