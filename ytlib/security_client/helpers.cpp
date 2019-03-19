#include "helpers.h"

#include "acl.h"

#include <yt/core/misc/error.h>

namespace NYT::NSecurityClient {

////////////////////////////////////////////////////////////////////////////////

ESecurityAction CheckPermissionsByAclAndSubjectClosure(
    const TSerializableAccessControlList& acl,
    const THashSet<TString>& subjectClosure,
    NYTree::EPermissionSet permissions)
{
    NYTree::EPermissionSet actualPermissions = {};
    for (const auto& ace : acl.Entries) {
        if (ace.Action != NSecurityClient::ESecurityAction::Allow) {
            THROW_ERROR_EXCEPTION("Action %Qv is not supported", FormatEnum(ace.Action));
        }
        for (const auto& aceSubject : ace.Subjects) {
            if (subjectClosure.contains(aceSubject)) {
                actualPermissions |= ace.Permissions;
                break;
            }
        }
    }
    return (actualPermissions & permissions) == permissions
        ? ESecurityAction::Allow
        : ESecurityAction::Deny;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSecurityClient
