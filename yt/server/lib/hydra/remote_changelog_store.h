#pragma once

#include "public.h"

#include <yt/client/api/public.h>

#include <yt/ytlib/transaction_client/public.h>

#include <yt/client/ypath/public.h>

#include <yt/core/profiling/public.h>

namespace NYT::NHydra {

////////////////////////////////////////////////////////////////////////////////

//! Creates a changelog store factory on top of DFS.
/*!
 *  If #prerequisiteTransactionId then the constructed stores are read-only.
 */
IChangelogStoreFactoryPtr CreateRemoteChangelogStoreFactory(
    TRemoteChangelogStoreConfigPtr config,
    TRemoteChangelogStoreOptionsPtr options,
    const NYPath::TYPath& path,
    NApi::IClientPtr client,
    NTransactionClient::TTransactionId prerequisiteTransactionId =
        NTransactionClient::NullTransactionId,
    const NProfiling::TTagIdList& profilerTags = NProfiling::EmptyTagIds);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra
