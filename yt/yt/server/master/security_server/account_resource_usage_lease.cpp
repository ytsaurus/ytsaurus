#include "account_resource_usage_lease.h"

#include <yt/yt/server/master/cell_master/serialize.h>

#include <yt/yt/server/master/security_server/account.h>

#include <yt/yt/server/master/transaction_server/transaction.h>

#include <yt/yt/core/misc/serialize.h>

namespace NYT::NSecurityServer {

using namespace NCellMaster;
using namespace NTransactionServer;

////////////////////////////////////////////////////////////////////////////////

TAccountResourceUsageLease::TAccountResourceUsageLease(
    TAccountResourceUsageLeaseId id,
    TTransaction* transaction,
    TAccount* account)
    : TObject(id)
    , Transaction_(transaction)
    , Account_(account)
{ }

TString TAccountResourceUsageLease::GetLowercaseObjectName() const
{
    return Format("account resource usage lease %v", GetId());
}

TString TAccountResourceUsageLease::GetCapitalizedObjectName() const
{
    return Format("Account usage lease %v", GetId());
}

TString TAccountResourceUsageLease::GetObjectPath() const
{
    return Format("//sys/account_resource_usage_leases/%v", GetId());
}

void TAccountResourceUsageLease::Save(TSaveContext& context) const
{
    TObject::Save(context);

    using NYT::Save;
    Save(context, CreationTime_);
    Save(context, Transaction_);
    Save(context, Account_);
    Save(context, Resources_);
}

void TAccountResourceUsageLease::Load(NCellMaster::TLoadContext& context)
{
    TObject::Load(context);

    using NYT::Load;
    Load(context, CreationTime_);
    Load(context, Transaction_);
    Load(context, Account_);
    Load(context, Resources_);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSecurityServer

