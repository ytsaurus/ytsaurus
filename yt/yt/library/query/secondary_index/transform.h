#pragma once

#include <yt/yt/library/query/base/ast.h>
#include <yt/yt/library/query/base/query_preparer.h>

#include <yt/yt/client/chaos_client/public.h>

#include <yt/yt/client/tablet_client/public.h>

namespace NYT::NQueryClient {

////////////////////////////////////////////////////////////////////////////////

void TransformWithIndexStatement(
    NAst::TQuery* query,
    const NTabletClient::ITableMountCachePtr& mountInfoCache,
    const NChaosClient::IReplicationCardCachePtr& replicationCardCache,
    TObjectsHolder* holder);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
