#pragma once

#include <yt/yt/library/query/base/ast.h>
#include <yt/yt/library/query/base/query_preparer.h>

#include <yt/yt/client/tablet_client/public.h>

namespace NYT::NQueryClient {

////////////////////////////////////////////////////////////////////////////////

void TransformWithIndexStatement(
    NAst::TQuery* query,
    const NTabletClient::ITableMountCachePtr& cache,
    TObjectsHolder* holder);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
