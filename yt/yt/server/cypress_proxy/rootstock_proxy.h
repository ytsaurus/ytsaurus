#pragma once

#include "public.h"

#include <yt/yt/ytlib/sequoia_client/public.h>

#include <yt/yt/core/ytree/public.h>

namespace NYT::NCypressProxy {

////////////////////////////////////////////////////////////////////////////////

NYTree::IYPathServicePtr CreateRootstockProxy(
    IBootstrap* bootstrap,
    NSequoiaClient::ISequoiaTransactionPtr transaction,
    NYPath::TYPath resolvedPath);

////////////////////////////////////////////////////////////////////////////////

} // namespace  NYT::NCypressProxy
