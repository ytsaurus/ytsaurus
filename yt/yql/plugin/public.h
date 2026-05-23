#pragma once

#include <library/cpp/yt/memory/ref_counted.h>

namespace NYT::NYqlPlugin {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TDynamicConfigManager)
DECLARE_REFCOUNTED_CLASS(TVanillaJobFile)
DECLARE_REFCOUNTED_CLASS(TDQYTBackend)
DECLARE_REFCOUNTED_CLASS(TDQYTCoordinator)
DECLARE_REFCOUNTED_STRUCT(TDQManagerConfig)
DECLARE_REFCOUNTED_STRUCT(TAdditionalSystemLib)
DECLARE_REFCOUNTED_STRUCT(TProcessYqlPluginConfig)
DECLARE_REFCOUNTED_STRUCT(TYqlPluginConfig)
DECLARE_REFCOUNTED_STRUCT(TRuntimeConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYqlPlugin
