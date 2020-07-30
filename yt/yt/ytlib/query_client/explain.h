#pragma once

#include "executor.h"

#include "public.h"

#include "query_preparer.h"

#include <yt/core/yson/string.h>

namespace NYT::NQueryClient {

////////////////////////////////////////////////////////////////////////////////

NYson::TYsonString BuildExplainQueryYson(
    NApi::NNative::IConnectionPtr connection,
    const TString& queryString,
    const std::unique_ptr<TPlanFragment>& fragment,
    TStringBuf udfRegistryPath);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient

