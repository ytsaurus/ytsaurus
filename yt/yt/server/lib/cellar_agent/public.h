#pragma once

#include <yt/yt/ytlib/cellar_client/public.h>

#include <yt/yt/client/election/public.h>

#include <yt/yt/core/misc/common.h>

#include <yt/yt/core/ypath/public.h>

namespace NYT::NCellarAgent {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TCellarManagerConfig)
DECLARE_REFCOUNTED_CLASS(TCellarConfig)
DECLARE_REFCOUNTED_CLASS(TCellarManagerDynamicConfig)
DECLARE_REFCOUNTED_CLASS(TCellarDynamicConfig)
DECLARE_REFCOUNTED_CLASS(TCellarOccupantConfig)

DECLARE_REFCOUNTED_STRUCT(ICellar)
DECLARE_REFCOUNTED_STRUCT(ICellarManager)
DECLARE_REFCOUNTED_STRUCT(ICellarOccupant)

DECLARE_REFCOUNTED_STRUCT(ICellarOccupier)
DECLARE_REFCOUNTED_STRUCT(ICellarOccupierProvider)

DECLARE_REFCOUNTED_STRUCT(ICellarBootstrapProxy)

// COMPAT(danilalexeev)
inline const NYPath::TYPath TabletCellCypressPrefix("//sys/tablet_cells");
inline const NYPath::TYPath ChaosCellCypressPrefix("//sys/chaos_cells");

inline const NYPath::TYPath CellsHydraPersistenceCypressPrefix("//sys/hydra_persistence");
inline const NYPath::TYPath TabletCellsHydraPersistenceCypressPrefix("//sys/hydra_persistence/tablet_cells");
inline const NYPath::TYPath ChaosCellsHydraPersistenceCypressPrefix("//sys/hydra_persistence/chaos_cells");

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellarAgent
