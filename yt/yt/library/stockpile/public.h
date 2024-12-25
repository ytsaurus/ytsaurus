#pragma once

#include <yt/yt/core/misc/configurable_singleton_decl.h>

#include <library/cpp/yt/memory/ref_counted.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TStockpileConfig)
DECLARE_REFCOUNTED_STRUCT(TStockpileDynamicConfig)

YT_DECLARE_RECONFIGURABLE_SINGLETON(TStockpileConfig, TStockpileDynamicConfig);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
