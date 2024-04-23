#pragma once

#include "public.h"

#include <yt/yt/ytlib/sequoia_client/public.h>

#include <yt/yt/core/ytree/public.h>

namespace NYT::NCypressProxy {

////////////////////////////////////////////////////////////////////////////////

ISequoiaServicePtr CreateNodeProxy(
    IBootstrap* bootstrap,
    NSequoiaClient::ISequoiaTransactionPtr transaction,
    NObjectClient::TObjectId id,
    NSequoiaClient::TAbsoluteYPath resolvedPath);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressProxy
