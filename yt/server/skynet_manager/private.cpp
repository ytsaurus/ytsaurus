#include "private.h"

#include <yt/core/logging/log.h>
#include <yt/core/profiling/profiler.h>

namespace NYT {
namespace NSkynetManager {

using namespace NYT::NLogging;
using namespace NYT::NProfiling;

////////////////////////////////////////////////////////////////////////////////

const TLogger SkynetManagerLogger{"SkynetManager"};
const TProfiler SkynetManagerProfiler{"/skynet_manager"};

////////////////////////////////////////////////////////////////////////////////

} // namespace NSkynetManager
} // namespace NYT
