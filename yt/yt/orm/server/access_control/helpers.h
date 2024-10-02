#pragma once

#include "public.h"

namespace NYT::NOrm::NServer::NAccessControl {

////////////////////////////////////////////////////////////////////////////////

bool ContainsPermission(
    const TAccessControlEntry& ace,
    TAccessControlPermissionValue permission);

template <class TFunction>
void ForEachAttributeAce(
    const TAccessControlList& acl,
    const NYPath::TYPath& attributePath,
    TFunction&& function);

std::optional<std::string> GetUserExecutionPool(const TClusterSubjectSnapshotPtr& snapshot);

std::optional<std::string> SelectExecutionPoolTag(
    const NAccessControl::TAccessControlManagerPtr& accessControlManager,
    const std::string& identityTag = "",
    bool userTagInsteadOfPool = false);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NAccessControl

#define HELPERS_INL_H_
#include "helpers-inl.h"
#undef HELPERS_INL_H_
