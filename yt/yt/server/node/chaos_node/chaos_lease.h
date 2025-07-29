#pragma once

#include "chaos_object_base.h"
#include "public.h"

#include <yt/yt/server/node/tablet_node/object_detail.h>

#include <yt/yt/client/chaos_client/public.h>

#include <library/cpp/yt/memory/ref_tracked.h>

namespace NYT::NChaosNode {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EChaosLeaseState,
    ((Normal)                           (0))
    ((RevokingShortcutsForRemoval)      (1))
    ((Migrated)                         (2))
);

class TChaosLease
    : public TChaosObjectBase
    , public TRefTracked<TChaosLease>
{
public:
    DEFINE_BYVAL_RW_PROPERTY(TChaosLeaseId, ParentId);
    DEFINE_BYREF_RW_PROPERTY(THashSet<TChaosLeaseId>, NestedLeases);

    DEFINE_BYVAL_RW_PROPERTY(TDuration, Timeout);
    DEFINE_BYVAL_RW_PROPERTY(EChaosLeaseState, State);

    DEFINE_BYREF_RW_PROPERTY(TPromise<void>, RemovePromise);

public:
    using TChaosObjectBase::TChaosObjectBase;

    void Save(TSaveContext& context) const;
    void Load(TLoadContext& context);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosNode
