#include "public.h"

#include "companion_manager.h"
#include "java_companion_manager.h"
#include "swift_map_companion_computation.h"
#include "swift_ordered_source_companion_computation.h"
#include "transform_companion_computation.h"

#include <yt/yt/flow/library/cpp/common/registry.h>

namespace NYT::NFlow::NCompanion {

////////////////////////////////////////////////////////////////////////////////

YT_FLOW_DEFINE_RESOURCE(TCompanionManager);
YT_FLOW_DEFINE_RESOURCE(TJavaCompanionManager);
YT_FLOW_DEFINE_COMPUTATION(TSwiftMapCompanionComputation);
YT_FLOW_DEFINE_COMPUTATION(TSwiftOrderedSourceCompanionComputation);
YT_FLOW_DEFINE_COMPUTATION(TTransformCompanionComputation);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NCompanion
