#include "private.h"

#include <yt/yt/client/tablet_client/public.h>

namespace NYT::NFlow::NController {

////////////////////////////////////////////////////////////////////////////////

bool IsTransientTabletError(const TError& error)
{
    return error.FindMatching(NTabletClient::EErrorCode::TabletServantIsNotActive) ||
        error.FindMatching(NTabletClient::EErrorCode::ReadOnlySmoothMovementStage);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NController
