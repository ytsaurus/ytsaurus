#pragma once

#include "public.h"

#include <core/rpc/public.h>

#include <ytlib/ypath/public.h>

#include <ytlib/election/public.h>

#include <ytlib/transaction_client/public.h>

namespace NYT {
namespace NHydra {

////////////////////////////////////////////////////////////////////////////////

ISnapshotStorePtr CreateRemoteSnapshotStore(
    TRemoteSnapshotStoreConfigPtr config,
    const NElection::TCellGuid& cellGuid,
    const NYPath::TYPath& remotePath,
    NRpc::IChannelPtr masterChannel,
    NTransactionClient::TTransactionManagerPtr transactionManager);

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT
