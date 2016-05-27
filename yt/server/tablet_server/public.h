#pragma once

#include <yt/server/hydra/public.h>

#include <yt/ytlib/hydra/public.h>

#include <yt/ytlib/tablet_client/public.h>

#include <yt/core/misc/enum.h>
#include <yt/core/misc/public.h>

namespace NYT {
namespace NTabletServer {

////////////////////////////////////////////////////////////////////////////////

using NHydra::TPeerId;
using NHydra::InvalidPeerId;
using NHydra::EPeerState;

using NTabletClient::TTabletCellBundleId;
using NTabletClient::NullTabletCellBundleId;
using NTabletClient::TTabletCellId;
using NTabletClient::NullTabletCellId;
using NTabletClient::TTabletId;
using NTabletClient::NullTabletId;
using NTabletClient::TStoreId;
using NTabletClient::ETabletState;
using NTabletClient::TypicalPeerCount;

using NTabletClient::TTabletCellConfig;
using NTabletClient::TTabletCellConfigPtr;
using NTabletClient::TTabletCellOptions;
using NTabletClient::TTabletCellOptionsPtr;

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(ETabletCellHealth,
    (Initializing)
    (Good)
    (Degraded)
    (Failed)
);

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TTabletManager)

DECLARE_REFCOUNTED_CLASS(TTabletManagerConfig)

DECLARE_ENTITY_TYPE(TTabletCellBundle, TTabletCellBundleId, NObjectClient::TDirectObjectIdHash)
DECLARE_ENTITY_TYPE(TTabletCell, TTabletCellId, NObjectClient::TDirectObjectIdHash)
DECLARE_ENTITY_TYPE(TTablet, TTabletId, NObjectClient::TDirectObjectIdHash)

struct TTabletStatistics;
struct TTabletPerformanceCounter;
struct TTabletPerformanceCounters;

extern const Stroka DefaultTabletCellBundleName;

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletServer
} // namespace NYT
