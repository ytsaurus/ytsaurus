#pragma once

#include "public.h"

#include <yt/core/yson/string.h>

#include <yt/ytlib/api/native/client_impl.h>

#include <yt/ytlib/query_client/query_preparer.h>

namespace NYT::NQueryClient {

using namespace NApi;

////////////////////////////////////////////////////////////////////////////////

NYson::TYsonString BuildExplainYson(
    const TString& queryString,
    const TExplainOptions& options,
    const std::unique_ptr<TPlanFragment>& fragment,
    const NNative::TClientPtr& client);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient

