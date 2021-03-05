#pragma once

#include <yt/yt/core/misc/common.h>

namespace NYT::NHttpProxy::NClickHouse {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TClickHouseHandler)

DECLARE_REFCOUNTED_CLASS(TStaticClickHouseConfig)
DECLARE_REFCOUNTED_CLASS(TDynamicClickHouseConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHttpProxy::NClickHouse
