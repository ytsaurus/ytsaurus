#pragma once

#include <yt/ytlib/hydra/public.h>

#include <yt/ytlib/object_client/public.h>

#include <yt/core/misc/public.h>

namespace NYT {
namespace NTabletClient {

///////////////////////////////////////////////////////////////////////////////

namespace NProto {

class TColumnFilter;
class TReqLookupRows;
class TReqWriteRow;
class TReqDeleteRow;

} // namespace NProto

///////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(ETabletState,
    ((Mounting)        (0))
    ((Mounted)         (1))
    ((Unmounting)      (2))
    ((Unmounted)       (3))
);

DEFINE_ENUM(EErrorCode,
    ((TransactionLockConflict)  (1700))
    ((NoSuchTablet)             (1701))
    ((TabletNotMounted)         (1702))
);

static const int TypicalPeerCount = 5;
static const int MaxPeerCount = 10;

///////////////////////////////////////////////////////////////////////////////

typedef NHydra::TCellId TTabletCellId;
extern const TTabletCellId NullTabletCellId;

typedef NObjectClient::TObjectId TTabletId;
extern const TTabletId NullTabletId;

typedef NObjectClient::TObjectId TStoreId;
extern const TStoreId NullStoreId;

typedef NObjectClient::TObjectId TPartitionId;
extern const TPartitionId NullPartitionId;

typedef NObjectClient::TObjectId TTabletCellBundleId;
extern const TTabletCellBundleId NullTabletCellBundleId;

///////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TTableMountInfo)
DECLARE_REFCOUNTED_STRUCT(TTabletInfo)
DECLARE_REFCOUNTED_CLASS(TTableMountCache)

DECLARE_REFCOUNTED_CLASS(TTabletCellOptions)
DECLARE_REFCOUNTED_CLASS(TTabletCellConfig)
DECLARE_REFCOUNTED_CLASS(TTableMountCacheConfig)

class TWireProtocolReader;
class TWireProtocolWriter;

///////////////////////////////////////////////////////////////////////////////

} // namespace NTabletClient
} // namespace NYT

