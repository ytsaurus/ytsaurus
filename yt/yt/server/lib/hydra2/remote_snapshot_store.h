#pragma once

#include <yt/yt/server/lib/hydra_common/public.h>

#include <yt/yt/client/api/public.h>

#include <yt/yt/ytlib/election/public.h>

#include <yt/yt/ytlib/transaction_client/public.h>

#include <yt/yt/client/ypath/public.h>

namespace NYT::NHydra2 {

////////////////////////////////////////////////////////////////////////////////

//! Creates a changelog store factory on top of DFS.
/*!
 *  If #prerequisiteTransactionId then the constructed stores are read-only.
 */
NHydra::ISnapshotStorePtr CreateRemoteSnapshotStore(
    NHydra::TRemoteSnapshotStoreConfigPtr config,
    NHydra::TRemoteSnapshotStoreOptionsPtr options,
    const NYPath::TYPath& path,
    NApi::IClientPtr client,
    NTransactionClient::TTransactionId prerequisiteTransactionId =
        NTransactionClient::NullTransactionId);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra2
