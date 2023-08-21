#pragma once

#include "public.h"

#include <yt/yt/server/master/cell_master/public.h>

#include <yt/yt/server/master/object_server/object_detail.h>

#include <yt/yt/server/master/security_server/public.h>

#include <yt/yt/server/master/transaction_server/public.h>

#include <yt/yt/core/misc/property.h>

namespace NYT::NObjectServer {

////////////////////////////////////////////////////////////////////////////////

class TStagedObject
    : public NObjectServer::TObject
{
public:
    DEFINE_BYVAL_RW_PROPERTY(NTransactionServer::TTransaction*, StagingTransaction);
    DEFINE_BYREF_RW_PROPERTY(NSecurityServer::TAccountPtr, StagingAccount);

public:
    using TObject::TObject;

    void CheckInvariants(NCellMaster::TBootstrap* bootstrap) const override;

    void Save(NCellMaster::TSaveContext& context) const;
    void Load(NCellMaster::TLoadContext& context);

    //! Returns True if the object is the staging area of some transaction.
    bool IsStaged() const;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NObjectServer
