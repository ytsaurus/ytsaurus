#include "helpers.h"

#include <yt/yt/core/misc/error.h>

namespace NYT::NSequoiaServer {

using namespace NCypressClient;

////////////////////////////////////////////////////////////////////////////////

TError CheckLockRequest(
    NCypressClient::ELockMode mode,
    const std::optional<std::string>& childKey,
    const std::optional<std::string>& attributeKey)
{
    if (mode != ELockMode::Snapshot &&
        mode != ELockMode::Shared &&
        mode != ELockMode::Exclusive)
    {
        return TError("Invalid lock mode %Qlv",
            mode);
    }

    if (childKey && attributeKey) {
        return TError(
            "Lock request cannot contain child key and attribute key at the same time");
    }

    if ((childKey || attributeKey) && mode != ELockMode::Shared) {
        return TError("Only shared locks support %v keys",
            childKey ? "child" : "attribute");
    }

    return {};
}

////////////////////////////////////////////////////////////////////////////////

EObjectType MaybeConvertToSequoiaType(EObjectType originalType)
{
    if (originalType == EObjectType::MapNode) {
        return EObjectType::SequoiaMapNode;
    }
    if (originalType == EObjectType::Link) {
        return EObjectType::SequoiaLink;
    }

    return originalType;
}

EObjectType MaybeConvertToCypressType(EObjectType originalType)
{
    if (originalType == EObjectType::SequoiaMapNode) {
        return EObjectType::MapNode;
    }
    if (originalType == EObjectType::SequoiaLink) {
        return EObjectType::Link;
    }

    return originalType;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSequoiaServer
