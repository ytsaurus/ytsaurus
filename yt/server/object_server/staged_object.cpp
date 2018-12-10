#include "staged_object.h"

#include <yt/server/cell_master/serialize.h>

#include <yt/server/security_server/account.h>

#include <yt/server/transaction_server/transaction.h>

namespace NYT::NObjectServer {

////////////////////////////////////////////////////////////////////////////////

TStagedObject::TStagedObject(const TObjectId& id)
    : TNonversionedObjectBase(id)
    , StagingTransaction_(nullptr)
    , StagingAccount_(nullptr)
{ }

void TStagedObject::Save(NCellMaster::TSaveContext& context) const
{
    TNonversionedObjectBase::Save(context);

    using NYT::Save;
    Save(context, StagingTransaction_);
    Save(context, StagingAccount_);
}

void TStagedObject::Load(NCellMaster::TLoadContext& context)
{
    TNonversionedObjectBase::Load(context);

    using NYT::Load;
    Load(context, StagingTransaction_);
    Load(context, StagingAccount_);
}

bool TStagedObject::IsStaged() const
{
    return StagingTransaction_ && StagingAccount_;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NObjectServer
