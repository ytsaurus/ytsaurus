#pragma once

#include <core/misc/common.h>

#include <ytlib/hydra/public.h>

#include <ytlib/object_client/public.h>

namespace NYT {
namespace NTabletClient {

///////////////////////////////////////////////////////////////////////////////

DECLARE_ENUM(ETabletState,
    ((Mounting)        (0))
    ((Mounted)         (1))
    ((Unmounting)      (2))
    ((Unmounted)       (3))
);

///////////////////////////////////////////////////////////////////////////////

typedef NHydra::TCellGuid TTabletCellId;
extern const TTabletCellId NullTabletCellId;

typedef NObjectClient::TObjectId TTabletId;
extern const TTabletId NullTabletId;

///////////////////////////////////////////////////////////////////////////////

struct TTableMountInfo;
typedef TIntrusivePtr<TTableMountInfo> TTableMountInfoPtr;

class TTableMountCache;
typedef TIntrusivePtr<TTableMountCache> TTableMountCachePtr;

class TTableMountCacheConfig;
typedef TIntrusivePtr<TTableMountCacheConfig> TTableMountCacheConfigPtr;

class TTableMountConfig;
typedef TIntrusivePtr<TTableMountConfig> TTableMountConfigPtr;

class TProtocolReader;
class TProtocolWriter;

///////////////////////////////////////////////////////////////////////////////

} // namespace NTabletClient
} // namespace NYT

