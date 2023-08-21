#pragma once

#include "public.h"
#include "tablet_owner_base.h"

#include <yt/yt/client/cypress_client/public.h>

namespace NYT::NTabletServer {

////////////////////////////////////////////////////////////////////////////////

class THunkStorageNode
    : public TTabletOwnerBase
    , public TRefTracked<THunkStorageNode>
{
public:
    using TTabletOwnerBase::TTabletOwnerBase;

    DEFINE_BYVAL_RW_PROPERTY(int, ReadQuorum);
    DEFINE_BYVAL_RW_PROPERTY(int, WriteQuorum);

    DEFINE_BYREF_RW_PROPERTY(THashSet<NCypressClient::TVersionedNodeId>, AssociatedNodeIds);

protected:
    using TBase = TTabletOwnerBase;

    TString GetLowercaseObjectName() const override;
    TString GetCapitalizedObjectName() const override;

    void Save(NCellMaster::TSaveContext& context) const override;
    void Load(NCellMaster::TLoadContext& context) override;

    void ValidateRemount() const override;

    void ValidateFreeze() const override;
    void ValidateUnfreeze() const override;
};

DEFINE_MASTER_OBJECT_TYPE(THunkStorageNode)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletServer
