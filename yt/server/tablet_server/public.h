#pragma once

#include <core/misc/common.h>
#include <core/misc/enum.h>

#include <ytlib/hydra/public.h>

#include <ytlib/tablet_client/public.h>

namespace NYT {
namespace NTabletServer {

////////////////////////////////////////////////////////////////////////////////

DECLARE_ENUM(ETabletCellState,
    ((Starting)   (0))
    ((Running)    (1))
);

DECLARE_ENUM(ETabletCellHealth,
    ((Initializing)(0))
    ((Good)        (1))
    ((Degraded)    (2))
    ((Failed)      (3))
);

////////////////////////////////////////////////////////////////////////////////

class TTabletManagerConfig;
typedef TIntrusivePtr<TTabletManagerConfig> TTabletManagerConfigPtr;

class TTabletCell;
class TTablet;

class TTabletManager;
typedef TIntrusivePtr<TTabletManager> TTabletManagerPtr;

////////////////////////////////////////////////////////////////////////////////

using NHydra::TPeerId;
using NHydra::InvalidPeerId;
using NHydra::EPeerState;

using NTabletClient::TTabletCellId;
using NTabletClient::NullTabletCellId;
using NTabletClient::TTabletId;
using NTabletClient::NullTabletId;
using NTabletClient::TStoreId;
using NTabletClient::ETabletState;
using NTabletClient::TypicalCellSize;

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletServer
} // namespace NYT
