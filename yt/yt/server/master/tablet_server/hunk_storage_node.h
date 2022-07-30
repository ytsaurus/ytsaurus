#pragma once

#include "public.h"
#include "tablet_owner_base.h"

namespace NYT::NTabletServer {

////////////////////////////////////////////////////////////////////////////////

class THunkStorageNode
    : public TTabletOwnerBase
{
public:
    using TTabletOwnerBase::TTabletOwnerBase;

    DEFINE_BYVAL_RW_PROPERTY(int, ReadQuorum);
    DEFINE_BYVAL_RW_PROPERTY(int, WriteQuorum);

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

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletServer
