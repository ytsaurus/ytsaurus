#include "permission_checker.h"
#include "helpers.h"

#include <library/cpp/yt/misc/enum.h>

namespace NYT::NSecurityServer {

using namespace NLogging;
using namespace NObjectClient;
using namespace NSecurityClient;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

namespace {

//! "Extend" permission in such way that when any read is requested (read or full_read),
//! we pretend that user requested `EPermission::Read | EPermission::FullRead`.
EPermissionSet ExtendReadPermission(EPermissionSet original) {
    auto anyhowRead = EPermission::Read | EPermission::FullRead;
    if (Any(original & anyhowRead)) {
        original = original | anyhowRead;
    }
    return original;
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

TPermissionChecker::TPermissionChecker(
    NYTree::EPermissionSet permissions,
    const TPermissionCheckBasicOptions& options,
    TLogger logger)
    : FullReadRequested_(Any(permissions & EPermission::FullRead))
    , PermissionsMask_(ExtendReadPermission(permissions))
    , Options_(options)
    , Logger(std::move(logger))
{
    YT_LOG_ALERT_IF(
        PopCount(permissions) > 1 && FullReadRequested_,
        "Checking \"full_read\" and some other permission (FullPermissions: %Qlv)",
        permissions);

    Response_.Action = ESecurityAction::Undefined;
    if (Options_.Columns) {
        for (const auto& column : *Options_.Columns) {
            // NB: Multiple occurrences are possible.
            Columns_.insert(column);
        }
    }
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
                if (result.Action == ESecurityAction::Undefined && !FullReadExplicitlyGranted_) {
                    result.Action = ESecurityAction::Deny;
                    if (!deniedColumnResult) {
                        deniedColumnResult = result;
                    }
                }
            }
        }

        if (FullReadRequested_ && deniedColumnResult) {
            SetDeny(deniedColumnResult->SubjectId, deniedColumnResult->ObjectId);
        }
    }

    if (FullReadExplicitlyGranted_) {
        // No need to mention RLACEs if we are allowed to FullRead.
        Response_.Rlaces.reset();
    }

    if (Response_.Rlaces && FullReadRequested_) {
        // NB(coteeq): Presence of RLACE alters the behaviour of non-row ACEs.
        // When RLACEs are present, non-row allowances do not actually allow FullRead.
        // This hack is not pretty, but it exists for RLACEs to be consistent with columnar ACEs.
        SetDeny(NullObjectId, NullObjectId);
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
    Response_.Rlaces.reset();
    Proceed_ = false;
}

bool TPermissionChecker::IsOnlyReadRequested() const
{
    return None(PermissionsMask_ & ~(EPermission::Read | EPermission::FullRead));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSecurityServer
