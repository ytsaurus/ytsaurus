#include "public.h"

#include "companion_manager.h"
#include "java_companion_manager.h"

#include <yt/yt/flow/library/cpp/common/registry.h>

namespace NYT::NFlow::NCompanion {

////////////////////////////////////////////////////////////////////////////////

YT_FLOW_DEFINE_RESOURCE(TCompanionManager);
YT_FLOW_DEFINE_RESOURCE(TJavaCompanionManager);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NCompanion
