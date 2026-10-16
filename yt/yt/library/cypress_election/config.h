#pragma once

#include "public.h"

#include <yt/yt/client/transaction_client/public.h>

#include <yt/yt/library/lock_election/config.h>

namespace NYT::NCypressElection {

////////////////////////////////////////////////////////////////////////////////

struct TCypressElectionManagerConfig
    : public NLockElection::TLockElectionManagerConfig
{
    NYPath::TYPath LockPath;

    TDuration TransactionTimeout;
    TDuration TransactionPingPeriod;
    NTransactionClient::EMasterTransactionExpirationMode MasterTransactionExpirationMode;
    TDuration LeaderCacheUpdatePeriod;

    REGISTER_YSON_STRUCT(TCypressElectionManagerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TCypressElectionManagerConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressElection
