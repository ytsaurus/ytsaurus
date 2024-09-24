#pragma once

#include <yt/yt/client/cypress_client/public.h>

namespace NYT::NSequoiaServer {

////////////////////////////////////////////////////////////////////////////////

TError CheckLockRequest(
    NCypressClient::ELockMode mode,
    const std::optional<TString>& childKey,
    const std::optional<TString>& attributeKey);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSequoiaServer
