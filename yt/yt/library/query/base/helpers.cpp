#include "helpers.h"

#include <yt/yt/client/tablet_client/public.h>

#include <yt/yt/core/concurrency/scheduler_api.h>

namespace NYT::NQueryClient {

using namespace NYT;

////////////////////////////////////////////////////////////////////////////////

#ifdef _asan_enabled_
static const int MinimumStackFreeSpace = 128_KB;
#else
static const int MinimumStackFreeSpace = 16_KB;
#endif

////////////////////////////////////////////////////////////////////////////////

void CheckStackDepth()
{
    if (!NConcurrency::CheckFreeStackSpace(MinimumStackFreeSpace)) {
        THROW_ERROR_EXCEPTION(
            NTabletClient::EErrorCode::QueryExpressionDepthLimitExceeded,
            "Expression depth causes stack overflow");
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
