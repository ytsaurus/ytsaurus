#pragma once

#include "public.h"

#include <yt/ytlib/api/public.h>

#include <yt/ytlib/election/public.h>

#include <yt/ytlib/transaction_client/public.h>

#include <yt/ytlib/ypath/public.h>

namespace NYT {
namespace NHydra {

////////////////////////////////////////////////////////////////////////////////

ISnapshotStorePtr CreateRemoteSnapshotStore(
    TRemoteSnapshotStoreConfigPtr config,
    TRemoteSnapshotStoreOptionsPtr options,
    const NYPath::TYPath& path,
    NApi::IClientPtr masterClient,
    const NTransactionClient::TTransactionId& prerequisiteTransactionId =
        NTransactionClient::NullTransactionId);

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT
