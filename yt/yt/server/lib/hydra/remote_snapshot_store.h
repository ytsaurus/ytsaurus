#pragma once

#include "public.h"

#include <yt/yt/ytlib/election/public.h>

#include <yt/yt/ytlib/transaction_client/public.h>

#include <yt/yt/client/api/public.h>

#include <yt/yt/client/ypath/public.h>

namespace NYT::NHydra {

////////////////////////////////////////////////////////////////////////////////

//! Creates a remote snapshot store.
/*!
 *  If #prerequisiteTransactionId then the constructed stores are read-only.
 */
ISnapshotStorePtr CreateRemoteSnapshotStore(
    TRemoteSnapshotStoreConfigPtr storeConfig,
    TRemoteSnapshotStoreOptionsPtr storeOptions,
    NYPath::TYPath primaryPath,
    NYPath::TYPath secondaryPath,
    NApi::IClientPtr client,
    NTransactionClient::TTransactionId prerequisiteTransactionId = {});

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra
