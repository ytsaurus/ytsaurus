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
    ((Good)       (0))
    ((Degraded)   (1))
    ((Failed)     (2))
);

static const int TypicalCellSize = 5;

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

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletServer
} // namespace NYT
