#pragma once

#include "public.h"

#include <yt/yt/server/lib/security_server/public.h>

#include <yt/yt/ytlib/transaction_client/public.h>

#include <yt/yt/client/api/client.h>

#include <yt/yt/ytlib/tablet_client/public.h>

#include <yt/yt/client/ypath/public.h>

namespace NYT::NHydra {

////////////////////////////////////////////////////////////////////////////////

//! Creates a changelog store factory on top of DFS.
/*!
 *  If #prerequisiteTransactionId then the constructed stores are read-only.
 */
IChangelogStoreFactoryPtr CreateRemoteChangelogStoreFactory(
    TRemoteChangelogStoreConfigPtr config,
    NTabletClient::TTabletCellOptionsPtr options,
    NYPath::TYPath primaryPath,
    NYPath::TYPath secondaryPath,
    NApi::IClientPtr client,
    NSecurityServer::IResourceLimitsManagerPtr resourceLimitsManager,
    NTransactionClient::TTransactionId prerequisiteTransactionId =
        NTransactionClient::NullTransactionId,
    const NApi::TJournalWriterPerformanceCounters& counters = {});

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra
