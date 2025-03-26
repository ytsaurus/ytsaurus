#pragma once

#include <yt/yt/core/misc/common.h>

namespace NYT::NHttpProxy::NClickHouse {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TClickHouseHandler)

DECLARE_REFCOUNTED_STRUCT(TStaticClickHouseConfig)
DECLARE_REFCOUNTED_STRUCT(TDynamicClickHouseConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHttpProxy::NClickHouse
