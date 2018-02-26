#include "client.h"

#include <yt/ytlib/scheduler/config.h>

namespace NYT {
namespace NApi {

////////////////////////////////////////////////////////////////////////////////

TSchedulingOptions::TSchedulingOptions()
{
    RegisterParameter("weight", Weight)
        .Default();
    RegisterParameter("resource_limits", ResourceLimits)
        .DefaultNew();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NApi
} // namespace NYT
