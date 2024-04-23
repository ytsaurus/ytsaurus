#pragma once

#include "public.h"

#include <yt/yt/ytlib/sequoia_client/public.h>

#include <yt/yt/core/ytree/public.h>

namespace NYT::NCypressProxy {

////////////////////////////////////////////////////////////////////////////////

ISequoiaServicePtr CreateRootstockProxy(
    IBootstrap* bootstrap,
    NSequoiaClient::ISequoiaTransactionPtr transaction,
    NSequoiaClient::TAbsoluteYPath resolvedPath);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressProxy
