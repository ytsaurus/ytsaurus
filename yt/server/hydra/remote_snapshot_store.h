#pragma once

#include "public.h"

#include <yt/client/api/public.h>

#include <yt/ytlib/election/public.h>

#include <yt/ytlib/transaction_client/public.h>

#include <yt/client/ypath/public.h>

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
    const NTransactionClient::TTransactionId& prerequisiteTransactionId =
        NTransactionClient::NullTransactionId);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra
