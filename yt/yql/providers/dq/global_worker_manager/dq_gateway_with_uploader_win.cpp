#include "dq_gateway_with_uploader.h"

namespace NYql {

TIntrusivePtr<IDqGateway> CreateDqGatewayWithUploader(
    TIntrusivePtr<IDqGateway> underlying,
    NActors::TActorSystem* /*actorSystem*/,
    TResourceManagerOptions /*uploadOptions*/,
    TIntrusivePtr<ICoordinationHelper> /*coordinator*/,
    TUploadClusterResolver /*resolveUploadCluster*/)
{
    // DQ clique client-side upload is not supported on Windows builds.
    return underlying;
}

} // namespace NYql
