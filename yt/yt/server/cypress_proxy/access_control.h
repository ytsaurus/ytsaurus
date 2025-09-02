#pragma once

#include "public.h"
#include "sequoia_session.h"

#include <yt/yt/server/lib/security_server/permission_checker.h>

namespace NYT::NCypressProxy {

////////////////////////////////////////////////////////////////////////////////

//! Represents a downward path to this node in a Sequoia tree.
//! See #TSequoiaResolveResult::NodeAncestry.
using TNodeAncestry = TRange<TCypressNodeDescriptor>;

NSecurityServer::TPermissionCheckResponse CheckPermissionForNode(
    const TSequoiaSessionPtr& sequoiaSession,
    TNodeAncestry nodeAncestry,
    NYTree::EPermission permission,
    const NSecurityServer::TPermissionCheckBasicOptions& options,
    TUserDirectoryPtr userDirectory);

void ValidatePermissionForNode(
    const TSequoiaSessionPtr& sequoiaSession,
    TNodeAncestry nodeAncestry,
    NYTree::EPermission permission,
    TUserDirectoryPtr userDirectory);

NSecurityServer::TPermissionCheckResult CheckPermissionForSubtree(
    const TSequoiaSessionPtr& sequoiaSession,
    TNodeAncestry nodeAncestry,
    const TSequoiaSession::TSubtree& nodeSubtree,
    NYTree::EPermission permission,
    bool descendantsOnly,
    TUserDirectoryPtr userDirectory);

void ValidatePermissionForSubtree(
    const TSequoiaSessionPtr& sequoiaSession,
    TNodeAncestry nodeAncestry,
    const TSequoiaSession::TSubtree& nodeSubtree,
    NYTree::EPermission permission,
    bool descendantsOnly,
    TUserDirectoryPtr userDirectory);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressProxy
