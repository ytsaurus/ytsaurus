#include "staged_object.h"

#include <yt/yt/server/master/cell_master/serialize.h>

#include <yt/yt/server/master/security_server/account.h>

#include <yt/yt/server/master/transaction_server/transaction.h>

#include <yt/yt/core/phoenix/type_def.h>

namespace NYT::NObjectServer {

using namespace NCellMaster;

////////////////////////////////////////////////////////////////////////////////

void TStagedObject::CheckInvariants(TBootstrap* bootstrap) const
{
    TObject::CheckInvariants(bootstrap);
}

void TStagedObject::RegisterMetadata(auto&& registrar)
{
    registrar.template BaseType<TObject>();

    PHOENIX_REGISTER_FIELD(1, StagingTransaction_);
    PHOENIX_REGISTER_FIELD(2, StagingAccount_);
}

PHOENIX_DEFINE_TYPE(TStagedObject);

bool TStagedObject::IsStaged() const
{
    return StagingTransaction_ && StagingAccount_;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NObjectServer
