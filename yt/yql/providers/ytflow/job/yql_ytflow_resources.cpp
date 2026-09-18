#include "yql_ytflow_computation_pattern_resource.h"
#include "yql_ytflow_function_registry_resource.h"

#include <yt/yt/flow/library/cpp/common/registry.h>

namespace NYql::NYtflow {

YT_FLOW_DEFINE_RESOURCE(TComputationPatternResource);
YT_FLOW_DEFINE_RESOURCE(TFunctionRegistryResource);

} // namespace NYql::NYtflow
