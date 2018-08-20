#pragma once

#include <yt/core/misc/public.h>

namespace NYT {
namespace NClickHouseProxy {

////////////////////////////////////////////////////////////////////////////////

class TBootstrap;

DECLARE_REFCOUNTED_CLASS(TClickHouseProxyConfig)
DECLARE_REFCOUNTED_CLASS(TClickHouseProxyServerConfig)
DECLARE_REFCOUNTED_CLASS(TClickHouseProxy)

////////////////////////////////////////////////////////////////////////////////

} // namespace NClickHouseProxy
} // namespace NYT
