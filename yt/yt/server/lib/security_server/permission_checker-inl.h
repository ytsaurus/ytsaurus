#ifndef PERMISSION_CHECKER_H
#error "Direct inclusion of this file is not allowed, include permission_checker.h"
// For the sake of sane code completion.
#include "permission_checker.h"
#endif

#include <yt/yt/client/security_client/acl.h>

namespace NYT::NSecurityServer {

////////////////////////////////////////////////////////////////////////////////

template <class TAccessControlEntry, class TCallback>
void TPermissionChecker::ProcessAce(
    const TAccessControlEntry& ace,
    const TCallback& matchAceSubjectCallback,
    NObjectClient::TObjectId objectId,
    int depth)
{
    if (!Proceed_) {
        return;
    }

    if (auto error = NSecurityClient::CheckAceCorrect(ace); !error.IsOK()) {
        YT_LOG_ALERT(
            error,
            "Got invalid ACE; skipping");
        return;
    }

    if (ace.Columns) {
        for (const auto& column : *ace.Columns) {
            auto it = Columns_.find(column);
            if (it == Columns_.end()) {
                continue;
            }
            // NB: Multiple occurrences are possible.
            ColumnToResult_.emplace(*it, TPermissionCheckResult());
        }
    }

    if (!CheckInheritanceMode(ace.InheritanceMode, depth)) {
        return;
    }

    if (None(ace.Permissions & PermissionsMask_)) {
        return;
    }

    if (PermissionsMask_ == NYTree::EPermission::RegisterQueueConsumer) {
        // RegisterQueueConsumer may only be present in ACE as a single permission;
        // in this case it is ensured that vitality is specified.
        YT_VERIFY(ace.Vital);
        if (!CheckVitalityMatch(*ace.Vital, Options_.Vital.value_or(false))) {
            return;
        }
    }

    for (auto subject : ace.Subjects) {
        auto adjustedSubject = matchAceSubjectCallback(subject);
        if (!adjustedSubject) {
            continue;
        }

        if (Any(ace.Permissions & NYTree::EPermission::FullRead)) {
            YT_VERIFY(ace.Action == NSecurityClient::ESecurityAction::Allow);
            FullReadExplicitlyGranted_ = true;
        }

        if (ace.Columns) {
            // XXX(coteeq): Maybe we should ban ACEs with columns and action=deny?
            // They do not seem to be helpful, but their absence may simplify
            // logic a bit.

            for (const auto& column : *ace.Columns) {
                auto it = ColumnToResult_.find(column);
                if (it == ColumnToResult_.end()) {
                    continue;
                }
                auto& columnResult = it->second;
                ProcessMatchingAceAction(
                    &columnResult,
                    ace.Action,
                    adjustedSubject,
                    objectId);
                if (FullReadRequested_ && columnResult.Action == NSecurityClient::ESecurityAction::Deny) {
                    SetDeny(adjustedSubject, objectId);
                    break;
                }
            }
        } else {
            ProcessMatchingAceAction(
                &Response_,
                ace.Action,
                adjustedSubject,
                objectId);
            if (Response_.Action == NSecurityClient::ESecurityAction::Deny) {
                SetDeny(adjustedSubject, objectId);
                break;
            }
        }

        if (!Proceed_) {
            break;
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSecurityServer
