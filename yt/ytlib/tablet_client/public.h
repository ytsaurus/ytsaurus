#pragma once

#include <core/misc/public.h>

#include <ytlib/hydra/public.h>

#include <ytlib/object_client/public.h>

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

DEFINE_ENUM(ELockMode,
    ((Row)             (0))
    ((Column)          (1))
);

static const int TypicalCellSize = 5;

///////////////////////////////////////////////////////////////////////////////

typedef NHydra::TCellId TTabletCellId;
extern const TTabletCellId NullTabletCellId;

typedef NObjectClient::TObjectId TTabletId;
extern const TTabletId NullTabletId;

typedef NObjectClient::TObjectId TStoreId;
extern const TStoreId NullStoreId;

typedef NObjectClient::TObjectId TPartitionId;
extern const TPartitionId NullPartitionId;

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

