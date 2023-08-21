#include "helpers.h"

#include <yt/yt/server/master/transaction_server/boomerang_tracker.h>

#include <yt/yt/server/lib/hive/hive_manager.h>

namespace NYT::NCellMaster {

////////////////////////////////////////////////////////////////////////////////

bool IsSubordinateMutation()
{
    return NHiveServer::IsHiveMutation() && !NTransactionServer::IsBoomerangMutation();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellMaster
