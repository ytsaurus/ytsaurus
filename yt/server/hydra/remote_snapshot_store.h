#pragma once

#include "public.h"

#include <ytlib/api/public.h>

#include <ytlib/ypath/public.h>

#include <ytlib/election/public.h>

#include <ytlib/transaction_client/public.h>

namespace NYT {
namespace NHydra {

////////////////////////////////////////////////////////////////////////////////

ISnapshotStorePtr CreateRemoteSnapshotStore(
    TRemoteSnapshotStoreConfigPtr config,
    TRemoteSnapshotStoreOptionsPtr options,
    const NYPath::TYPath& path,
    NApi::IClientPtr masterClient,
    const std::vector<NTransactionClient::TTransactionId>& prerequisiteTransactionIds =
        std::vector<NTransactionClient::TTransactionId>());

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT
