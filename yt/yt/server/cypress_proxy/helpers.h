#pragma once

#include "public.h"

#include <yt/yt/client/cypress_client/public.h>

#include <yt/yt/client/object_client/public.h>

#include <yt/yt/core/ytree/public.h>

namespace NYT::NCypressProxy {

////////////////////////////////////////////////////////////////////////////////

std::vector<NYPath::TYPathBuf> TokenizeUnresolvedSuffix(const NYPath::TYPath& unresolvedSuffix);

NYPath::TYPath GetJoinedNestedNodesPath(
    const NYPath::TYPath& parentPath,
    const std::vector<NYPath::TYPathBuf>& childKeys);

bool IsSupportedSequoiaType(NCypressClient::EObjectType type);

bool IsSequoiaCompositeNodeType(NCypressClient::EObjectType type);

void ValidateSupportedSequoiaType(NCypressClient::EObjectType type);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressProxy
