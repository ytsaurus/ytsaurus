#pragma once

#include "public.h"

#include "hunk_tablet.h"

namespace NYT::NTabletNode {

////////////////////////////////////////////////////////////////////////////////

struct IHunkTabletHost
    : public virtual TRefCounted
{
    virtual void ScheduleScanTablet(TTabletId tabletId) = 0;
};

DEFINE_REFCOUNTED_TYPE(IHunkTabletHost)

////////////////////////////////////////////////////////////////////////////////

struct IHunkTabletManager
    : public virtual TRefCounted
{
    virtual void Initialize() = 0;

    DECLARE_INTERFACE_ENTITY_MAP_ACCESSORS(Tablet, THunkTablet);

    virtual THunkTablet* GetTabletOrThrow(TTabletId tabletId) = 0;

    virtual void CheckFullyUnlocked(THunkTablet* tablet) = 0;

    virtual NYTree::IYPathServicePtr GetOrchidService() = 0;
};

DEFINE_REFCOUNTED_TYPE(IHunkTabletManager)

////////////////////////////////////////////////////////////////////////////////

IHunkTabletManagerPtr CreateHunkTabletManager(IBootstrap* bootstrap, ITabletSlotPtr slot);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
