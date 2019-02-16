#pragma once

#include <yt/core/misc/public.h>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

namespace NProto {

class TReadJobSpec;

}

////////////////////////////////////////////////////////////////////////////////

extern TString ClickHouseUserName;

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
DECLARE_SHARED_STRUCT(IStorage);
DECLARE_SHARED_STRUCT(ITableReader);
DECLARE_SHARED_STRUCT(TTable);

struct IPathService;
struct IAuthorizationTokenService;
struct TColumn;
struct TValue;
class TBootstrap;

#undef DELCARE_SHARED_STRUCT

////////////////////////////////////////////////////////////////////////////////

class TReadJobSpec;

DECLARE_REFCOUNTED_CLASS(TEngineConfig);
DECLARE_REFCOUNTED_CLASS(TClickHouseServerBootstrapConfig);
DECLARE_REFCOUNTED_CLASS(TUserConfig);
DECLARE_REFCOUNTED_CLASS(TNativeClientCacheConfig);

DECLARE_REFCOUNTED_STRUCT(ISubscriptionManager);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
