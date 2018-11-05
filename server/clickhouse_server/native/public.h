#pragma once

#include <yt/core/misc/public.h>

namespace NYT {
namespace NClickHouseServer {
namespace NNative {

////////////////////////////////////////////////////////////////////////////////

namespace NProto {

class TReadJobSpec;

}

////////////////////////////////////////////////////////////////////////////////

#define DECLARE_SHARED_STRUCT(TStruct) \
    struct TStruct; \
    using TStruct ## Ptr = std::shared_ptr<TStruct>

DECLARE_SHARED_STRUCT(IAuthorizationToken);
DECLARE_SHARED_STRUCT(ICliqueAuthorizationManager);
DECLARE_SHARED_STRUCT(IColumnBuilder);
DECLARE_SHARED_STRUCT(ICoordinationService);
DECLARE_SHARED_STRUCT(IDocument);
DECLARE_SHARED_STRUCT(ILogger);
DECLARE_SHARED_STRUCT(IRangeFilter);
DECLARE_SHARED_STRUCT(IStorage);
DECLARE_SHARED_STRUCT(ITableReader);
DECLARE_SHARED_STRUCT(TTable);

struct IPathService;
struct IAuthorizationTokenService;
struct TColumn;
struct TValue;

#undef DELCARE_SHARED_STRUCT

////////////////////////////////////////////////////////////////////////////////

class TReadJobSpec;

DECLARE_REFCOUNTED_CLASS(TConfig);
DECLARE_REFCOUNTED_CLASS(TNativeClientCacheConfig);

DECLARE_REFCOUNTED_STRUCT(ISubscriptionManager);
DECLARE_REFCOUNTED_STRUCT(INativeClientCache);

////////////////////////////////////////////////////////////////////////////////

} // namespace NNative
} // namespace NClickHouseServer
} // namespace NYT
