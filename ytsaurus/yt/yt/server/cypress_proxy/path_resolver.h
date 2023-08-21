#pragma once

#include "public.h"

#include <yt/yt/ytlib/sequoia_client/public.h>

#include <yt/yt/client/cypress_client/public.h>

#include <yt/yt/core/ytree/public.h>

namespace NYT::NCypressProxy {

////////////////////////////////////////////////////////////////////////////////

struct TCypressResolveResult
{ };

struct TSequoiaResolveResult
{
    NYTree::TYPath ResolvedPrefix;
    NCypressClient::TNodeId ResolvedPrefixNodeId;

    NYTree::TYPath UnresolvedSuffix;
};

using TResolveResult = std::variant<
    TCypressResolveResult,
    TSequoiaResolveResult
>;

TResolveResult ResolvePath(
    NSequoiaClient::ISequoiaTransactionPtr transaction,
    NYTree::TYPath path);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressProxy
