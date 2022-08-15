#pragma once

#include "executor.h"

#include <yt/yt/library/query/base/public.h>

#include <yt/yt/library/query/base/query_preparer.h>

#include <yt/yt/core/yson/string.h>

#include <yt/yt/client/api/client.h>

namespace NYT::NQueryClient {

////////////////////////////////////////////////////////////////////////////////

NYson::TYsonString BuildExplainQueryYson(
    NApi::NNative::IConnectionPtr connection,
    const std::unique_ptr<TPlanFragment>& fragment,
    TStringBuf udfRegistryPath,
    const NApi::TExplainQueryOptions& options);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient

