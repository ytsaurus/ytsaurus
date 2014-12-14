#include "stdafx.h"
#include "staged_object.h"

#include <server/transaction_server/transaction.h>

#include <server/security_server/account.h>

#include <server/cell_master/serialize.h>

namespace NYT {
namespace NObjectServer {

////////////////////////////////////////////////////////////////////////////////

TStagedObject::TStagedObject()
    : StagingTransaction_(nullptr)
    , StagingAccount_(nullptr)
{ }

void TStagedObject::Save(NCellMaster::TSaveContext& context) const
{
    using NYT::Save;
    Save(context, StagingTransaction_);
    Save(context, StagingAccount_);
}

void TStagedObject::Load(NCellMaster::TLoadContext& context)
{
    using NYT::Load;
    Load(context, StagingTransaction_);
    Load(context, StagingAccount_);
}

bool TStagedObject::IsStaged() const
{
    return StagingTransaction_ && StagingAccount_;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjectServer
} // namespace NYT
