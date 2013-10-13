#pragma once

#include <core/misc/common.h>

#include <ytlib/election/public.h>

#include <server/hydra/public.h>

#include <server/tablet_server/public.h>

namespace NYT {
namespace NTabletNode {

////////////////////////////////////////////////////////////////////////////////

using NElection::TCellGuid;
using NElection::NullCellGuid;

using NTabletServer::TTabletCellId;
using NTabletServer::TTabletId;

typedef NHydra::TSaveContext TSaveContext;
typedef NHydra::TLoadContext TLoadContext;

////////////////////////////////////////////////////////////////////////////////
    
class TTabletNodeConfig;
typedef TIntrusivePtr<TTabletNodeConfig> TTabletNodeConfigPtr;

class TTabletCellController;
typedef TIntrusivePtr<TTabletCellController> TTabletCellControllerPtr;

class TTabletSlot;
typedef TIntrusivePtr<TTabletSlot> TTabletSlotPtr;

class TSlotAutomaton;
typedef TIntrusivePtr<TSlotAutomaton> TSlotAutomatonPtr;

class TTabletManager;
typedef TIntrusivePtr<TTabletManager> TTabletManagerPtr;

class TTablet;

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT
