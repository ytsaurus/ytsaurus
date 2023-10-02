#pragma once

#include "public.h"

#include <yt/yt/ytlib/sequoia_client/public.h>

#include <yt/yt/core/ytree/public.h>

namespace NYT::NCypressProxy {

////////////////////////////////////////////////////////////////////////////////

NYTree::IYPathServicePtr CreateNodeProxy(
    IBootstrap* bootstrap,
    NSequoiaClient::ISequoiaTransactionPtr transaction,
    NObjectClient::TObjectId id,
    NYPath::TYPath resolvedPath);

////////////////////////////////////////////////////////////////////////////////

} // namespace  NYT::NCypressProxy
