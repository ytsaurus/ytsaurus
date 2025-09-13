#pragma once

#include "public.h"
#include "sequoia_session.h"

#include <yt/yt/server/lib/security_server/permission_checker.h>

namespace NYT::NCypressProxy {

////////////////////////////////////////////////////////////////////////////////

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

NSecurityClient::TSerializableAccessControlList ComputeEffectiveAclForNode(
    const TSequoiaSessionPtr& sequoiaSession,
    TNodeAncestry nodeAncestry);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressProxy
