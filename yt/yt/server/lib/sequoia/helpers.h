#pragma once

#include <yt/yt/client/cypress_client/public.h>

namespace NYT::NSequoiaServer {

////////////////////////////////////////////////////////////////////////////////

TError CheckLockRequest(
    NCypressClient::ELockMode mode,
    const std::optional<std::string>& childKey,
    const std::optional<std::string>& attributeKey);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSequoiaServer
