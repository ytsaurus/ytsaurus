#pragma once

#include <core/misc/common.h>

#include <ytlib/hydra/public.h>

#include <ytlib/object_client/public.h>

namespace NYT {
namespace NTabletClient {

///////////////////////////////////////////////////////////////////////////////

typedef NHydra::TCellGuid TTabletCellId;
extern const TTabletCellId NullTabletCellId;

typedef NObjectClient::TObjectId TTabletId;
extern const TTabletId NullTabletId;

///////////////////////////////////////////////////////////////////////////////

class TTableMountConfig;
typedef TIntrusivePtr<TTableMountConfig> TTableMountConfigPtr;

///////////////////////////////////////////////////////////////////////////////

} // namespace NTabletClient
} // namespace NYT

