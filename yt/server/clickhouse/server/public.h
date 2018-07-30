#pragma once

#include <yt/core/misc/public.h>

namespace NYT {
namespace NClickHouse {

namespace NProto {
    class TReadJobSpec;
}

////////////////////////////////////////////////////////////////////////////////

class TReadJobSpec;

DECLARE_REFCOUNTED_CLASS(TConfig);
DECLARE_REFCOUNTED_CLASS(TNativeClientCacheConfig);

DECLARE_REFCOUNTED_STRUCT(ISubscriptionManager);
DECLARE_REFCOUNTED_STRUCT(INativeClientCache);

}   // namespace NClickHouse
}   // namespace NYT
