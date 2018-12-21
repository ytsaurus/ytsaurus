#pragma once

#include "public.h"

#include <yt/server/cell_master/public.h>

#include <yt/server/object_server/object_detail.h>

#include <yt/server/security_server/public.h>

#include <yt/server/transaction_server/public.h>

#include <yt/core/misc/property.h>

namespace NYT::NObjectServer {

////////////////////////////////////////////////////////////////////////////////

class TStagedObject
    : public NObjectServer::TNonversionedObjectBase
{
public:
    DEFINE_BYVAL_RW_PROPERTY(NTransactionServer::TTransaction*, StagingTransaction);
    DEFINE_BYVAL_RW_PROPERTY(NSecurityServer::TAccount*, StagingAccount);

public:
    explicit TStagedObject(TObjectId id);

    void Save(NCellMaster::TSaveContext& context) const;
    void Load(NCellMaster::TLoadContext& context);

    //! Returns True if the object is the staging area of some transaction.
    bool IsStaged() const;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NObjectServer
