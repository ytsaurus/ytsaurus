#pragma once

#include <yt/yql/providers/dq/config/config.pb.h>
#include <yt/yql/providers/dq/actors/yt/resource_manager.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NYql {

TVector<TResourceManagerOptions> BuildWarmupYtBackendsFromDqConfig(const NProto::TDqConfig& config);

} // namespace NYql
