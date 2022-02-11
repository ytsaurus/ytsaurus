#include "staged_object.h"

#include <yt/yt/server/master/cell_master/serialize.h>

#include <yt/yt/server/master/security_server/account.h>

#include <yt/yt/server/master/transaction_server/transaction.h>

namespace NYT::NObjectServer {

using namespace NCellMaster;

////////////////////////////////////////////////////////////////////////////////

void TStagedObject::CheckInvariants(TBootstrap* bootstrap) const
{
    TObject::CheckInvariants(bootstrap);
}

void TStagedObject::Save(TSaveContext& context) const
{
    TObject::Save(context);

    using NYT::Save;
    Save(context, StagingTransaction_);
    Save(context, StagingAccount_);
}

void TStagedObject::Load(TLoadContext& context)
{
    TObject::Load(context);

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
