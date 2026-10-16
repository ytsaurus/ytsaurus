#pragma once

#include <library/cpp/yt/memory/ref_counted.h>

namespace NYT::NYqlPlugin {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TDynamicConfigManager)
DECLARE_REFCOUNTED_STRUCT(TVanillaJobFile)
DECLARE_REFCOUNTED_STRUCT(TDQYTBackend)
DECLARE_REFCOUNTED_STRUCT(TDQYTCoordinator)
DECLARE_REFCOUNTED_STRUCT(TDQManagerConfig)
DECLARE_REFCOUNTED_STRUCT(TAdditionalSystemLib)
DECLARE_REFCOUNTED_STRUCT(TProcessYqlPluginConfig)
DECLARE_REFCOUNTED_STRUCT(TYqlPluginConfig)
DECLARE_REFCOUNTED_STRUCT(TYqlPluginDynamicConfig)
DECLARE_REFCOUNTED_STRUCT(TUdfMeta)
DECLARE_REFCOUNTED_STRUCT(TUdfEntryMeta)
DECLARE_REFCOUNTED_STRUCT(TUdfModuleMeta)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYqlPlugin
