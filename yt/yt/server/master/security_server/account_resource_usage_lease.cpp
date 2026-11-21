#include "account_resource_usage_lease.h"

#include <yt/yt/server/master/cell_master/serialize.h>

#include <yt/yt/server/master/security_server/account.h>

#include <yt/yt/server/master/transaction_server/transaction.h>

#include <yt/yt/core/misc/serialize.h>

#include <yt/yt/core/phoenix/type_def.h>

namespace NYT::NSecurityServer {

using namespace NCellMaster;
using namespace NTransactionServer;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TAccountResourceUsageLease::TAccountResourceUsageLease(
    TAccountResourceUsageLeaseId id,
    TTransaction* transaction,
    TAccount* account)
    : TObject(id)
    , Transaction_(transaction)
    , Account_(account)
{ }

std::string TAccountResourceUsageLease::GetLowercaseObjectName() const
{
    return Format("account resource usage lease %v", GetId());
}

std::string TAccountResourceUsageLease::GetCapitalizedObjectName() const
{
    return Format("Account usage lease %v", GetId());
}

TYPath TAccountResourceUsageLease::GetObjectPath() const
{
    return Format("//sys/account_resource_usage_leases/%v", GetId());
}

void TAccountResourceUsageLease::RegisterMetadata(auto&& registrar)
{
    registrar.template BaseType<TObject>();

    PHOENIX_REGISTER_FIELD(1, CreationTime_);
    PHOENIX_REGISTER_FIELD(2, Transaction_);
    PHOENIX_REGISTER_FIELD(3, Account_);
    PHOENIX_REGISTER_FIELD(4, Resources_);
}

PHOENIX_DEFINE_TYPE(TAccountResourceUsageLease);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSecurityServer

