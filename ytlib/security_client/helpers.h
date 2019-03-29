#pragma once

#include "public.h"

#include <yt/core/ytree/permission.h>

namespace NYT::NSecurityClient {

////////////////////////////////////////////////////////////////////////////////

ESecurityAction CheckPermissionsByAclAndSubjectClosure(
    const TSerializableAccessControlList& acl,
    const THashSet<TString>& subjectClosure,
    NYTree::EPermissionSet permissions);

void ValidateSecurityTag(const TSecurityTag& tag);
void ValidateSecurityTags(const std::vector<TSecurityTag>& tags);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSecurityClient

