#include "permission_checker.h"
#include "helpers.h"

#include <library/cpp/yt/misc/enum.h>

namespace NYT::NSecurityServer {

using namespace NObjectClient;
using namespace NSecurityClient;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TPermissionChecker::TPermissionChecker(
    NYTree::EPermission permission,
    const TPermissionCheckBasicOptions& options)
    : FullRead_(Any(permission & EPermission::FullRead))
    , Permission_(FullRead_
        ? permission & ~EPermission::FullRead | EPermission::Read
        : permission)
    , Options_(options)
{
    Response_.Action = ESecurityAction::Undefined;
    if (Options_.Columns) {
        for (const auto& column : *Options_.Columns) {
            // NB: Multiple occurrences are possible.
            Columns_.insert(column);
        }
    }
    Proceed_ = true;
}

bool TPermissionChecker::ShouldProceed() const
{
    return Proceed_;
}

TPermissionCheckResponse TPermissionChecker::GetResponse()
{
    if (Response_.Action == ESecurityAction::Undefined) {
        SetDeny(NullObjectId, NullObjectId);
    }

    if (Response_.Action == ESecurityAction::Allow && Options_.Columns) {
        Response_.Columns = std::vector<TPermissionCheckResult>(Options_.Columns->size());
        std::optional<TPermissionCheckResult> deniedColumnResult;
        for (size_t index = 0; index < Options_.Columns->size(); ++index) {
            const auto& column = (*Options_.Columns)[index];
            auto& result = (*Response_.Columns)[index];
            auto it = ColumnToResult_.find(column);
            if (it == ColumnToResult_.end()) {
                result = static_cast<const TPermissionCheckResult>(Response_);
            } else {
                result = it->second;
                if (result.Action == ESecurityAction::Undefined) {
                    result.Action = ESecurityAction::Deny;
                    if (!deniedColumnResult) {
                        deniedColumnResult = result;
                    }
                }
            }
        }

        if (FullRead_ && deniedColumnResult) {
            SetDeny(deniedColumnResult->SubjectId, deniedColumnResult->ObjectId);
        }
    }

    return std::move(Response_);
}

bool TPermissionChecker::CheckInheritanceMode(EAceInheritanceMode mode, int depth)
{
    return GetInheritedInheritanceMode(mode, depth).has_value();
}

bool TPermissionChecker::CheckVitalityMatch(bool vital, bool requestedVital)
{
    return !requestedVital || vital;
}

void TPermissionChecker::ProcessMatchingAceAction(
    TPermissionCheckResult* result,
    ESecurityAction action,
    TSubjectId subjectId,
    TObjectId objectId)
{
    if (result->Action == ESecurityAction::Deny) {
        return;
    }

    result->Action = action;
    result->ObjectId = objectId;
    result->SubjectId = subjectId;
}

void TPermissionChecker::SetDeny(
    TPermissionCheckResult* result,
    TSubjectId subjectId,
    TObjectId objectId)
{
    result->Action = ESecurityAction::Deny;
    result->SubjectId = subjectId;
    result->ObjectId = objectId;
}

void TPermissionChecker::SetDeny(TSubjectId subjectId, TObjectId objectId)
{
    SetDeny(&Response_, subjectId, objectId);
    if (Response_.Columns) {
        for (auto& result : *Response_.Columns) {
            SetDeny(&result, subjectId, objectId);
        }
    }
    Proceed_ = false;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSecurityServer
