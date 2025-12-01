#pragma once

#include "public.h"
#include "sequoia_session.h"

#include <yt/yt/server/lib/security_server/permission_checker.h>

namespace NYT::NCypressProxy {

////////////////////////////////////////////////////////////////////////////////

using TMatchAceSubjectCallback = std::function<
    NSecurityClient::TSubjectId(const NSecurityClient::TSerializableAccessControlEntry&)>;

TMatchAceSubjectCallback CreateMatchAceSubjectCallback(
    TUserDescriptorPtr user,
    TUserDirectoryPtr userDirectory);

NSecurityClient::ESecurityAction FastCheckPermission(const TUserDescriptorPtr& user);

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

// Repressents an intermediate read access permission check result in
// an append-only ACD stack.
class TIntermediateReadPermissionCheckResult
{
public:
    TIntermediateReadPermissionCheckResult() = default;

    NSecurityClient::ESecurityAction GetAction() const;

    TIntermediateReadPermissionCheckResult Put(
        const TAccessControlDescriptor& acd,
        const TMatchAceSubjectCallback& matchAceSubjectCallback) const;

private:
    NSecurityClient::ESecurityAction ObjectAction_ = NSecurityClient::ESecurityAction::Undefined;
    NSecurityClient::ESecurityAction ImmediateDescendantsAction_ = NSecurityClient::ESecurityAction::Undefined;
    NSecurityClient::ESecurityAction DescendatsAction_ = NSecurityClient::ESecurityAction::Undefined;

    TIntermediateReadPermissionCheckResult(
        NSecurityClient::ESecurityAction objectAction,
        NSecurityClient::ESecurityAction immediateDescendantsAction,
        NSecurityClient::ESecurityAction descendatsAction);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressProxy
