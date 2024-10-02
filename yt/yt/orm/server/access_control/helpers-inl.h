#ifndef HELPERS_INL_H_
#error "Direct inclusion of this file is not allowed, include helpers.h"
// For the sake of sane code completion.
#include "helpers.h"
#endif

namespace NYT::NOrm::NServer::NAccessControl {

////////////////////////////////////////////////////////////////////////////////

template <class TFunction>
void ForEachAttributeAce(
    const TAccessControlList& acl,
    const NYPath::TYPath& attributePath,
    TFunction&& function)
{
    for (const auto& ace : acl) {
        const auto& attributes = ace.Attributes;
        bool shouldRun = false;
        if (attributes.empty()) {
            shouldRun = true;
        } else {
            for (const auto& attribute : attributes) {
                if (attributePath.StartsWith(attribute)) {
                    shouldRun = true;
                    break;
                }
            }
        }
        if (shouldRun) {
            function(ace);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NAccessControl
