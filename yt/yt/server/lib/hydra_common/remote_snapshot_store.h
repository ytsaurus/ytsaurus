#pragma once

#include "public.h"

#include <yt/yt/client/api/public.h>

#include <yt/yt/ytlib/election/public.h>

#include <yt/yt/ytlib/transaction_client/public.h>

#include <yt/yt/client/ypath/public.h>

namespace NYT::NHydra {

////////////////////////////////////////////////////////////////////////////////

//! Creates a changelog store factory on top of DFS.
/*!
 *  If #prerequisiteTransactionId then the constructed stores are read-only.
 */
ISnapshotStorePtr CreateRemoteSnapshotStore(
    TRemoteSnapshotStoreConfigPtr config,
    TRemoteSnapshotStoreOptionsPtr options,
    const NYPath::TYPath& path,
    NApi::IClientPtr client,
    NTransactionClient::TTransactionId prerequisiteTransactionId =
        NTransactionClient::NullTransactionId);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra
