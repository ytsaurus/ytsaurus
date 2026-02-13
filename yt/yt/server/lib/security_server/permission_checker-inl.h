#ifndef PERMISSION_CHECKER_INL_H_
#error "Direct inclusion of this file is not allowed, include permission_checker.h"
// For the sake of sane code completion.
#include "permission_checker.h"
#endif

#include "helpers.h"

namespace NYT::NSecurityServer {

////////////////////////////////////////////////////////////////////////////////

template <class TAccessControlEntry, NDetail::CSubjectMatchCallback<TAccessControlEntry> TCallback>
TPermissionChecker<TAccessControlEntry, TCallback>::TPermissionChecker(
    NYTree::EPermissionSet permissions,
    TCallback matchAceSubjectCallback,
    const TPermissionCheckBasicOptions* options)
    : FullReadRequested_(Any(permissions & NYTree::EPermission::FullRead))
    , PermissionsMask_(NDetail::ExtendReadPermission(permissions))
    , Options_(options)
    , MatchAceSubjectCallback_(std::move(matchAceSubjectCallback))
{
    Response_.Action = NSecurityClient::ESecurityAction::Undefined;
    if (Options_->Columns) {
        for (const auto& column : *Options_->Columns) {
            // NB: Multiple occurrences are possible.
            Columns_.insert(column);
        }
    }
}

template <class TAccessControlEntry, NDetail::CSubjectMatchCallback<TAccessControlEntry> TCallback>
bool TPermissionChecker<TAccessControlEntry, TCallback>::ShouldProceed() const
{
    return ShouldProceed_;
}

template <class TAccessControlEntry, NDetail::CSubjectMatchCallback<TAccessControlEntry> TCallback>
void TPermissionChecker<TAccessControlEntry, TCallback>::ProcessAce(
    const TAccessControlEntry& ace,
    NObjectClient::TObjectId objectId,
    int depth)
{
    if (!ShouldProceed_) {
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
        if (!CheckVitalityMatch(*ace.Vital, Options_->Vital.value_or(false))) {
            return;
        }
    }

    // NB(coteeq): Generally, Row-Level ACE presence means that some action
    // should be partially denied (e.g. read-like action must return only
    // a subset of rows). We do not want to break this rule, so we do not want
    // to return RL ACEs while checking non-read permissions (the logic
    // in GetBasicAttributes also relies on this rule).
    //
    // At this point, we already checked that caller requested relevant permission.
    // Keeping in mind that row-level ACE must have exactly ["read"] permissions,
    // we can be sure that the caller must have requested read, so we can safely
    // emplace into RowLevelAcl in the response.
    if (ace.RowAccessPredicate && !Response_.RowLevelAcl) {
        Response_.HasRowLevelAce = true;
        Response_.RowLevelAcl.emplace();
    }

    auto subjectId = MatchAceSubjectCallback_(ace);
    if (!subjectId) {
        return;
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
                subjectId,
                objectId);
            if (FullReadRequested_ && columnResult.Action == NSecurityClient::ESecurityAction::Deny) {
                RequestedFullReadButReadIsDenied_ = false;
                SetDeny(subjectId, objectId);
                break;
            }
        }
    } else if (ace.RowAccessPredicate) {
        Response_.RowLevelAcl->emplace_back(
            *ace.RowAccessPredicate,
            ace.InapplicableRowAccessPredicateMode
                .value_or(NSecurityClient::EInapplicableRowAccessPredicateMode::Fail));
    } else {
        ProcessMatchingAceAction(
            &Response_,
            ace.Action,
            subjectId,
            objectId);
        if (Response_.Action == NSecurityClient::ESecurityAction::Deny) {
            SetDeny(subjectId, objectId);
        }
    }
}

template <class TAccessControlEntry, NDetail::CSubjectMatchCallback<TAccessControlEntry> TCallback>
TPermissionCheckResponse TPermissionChecker<TAccessControlEntry, TCallback>::GetResponse() &&
{
    if (Response_.Action == NSecurityClient::ESecurityAction::Undefined && !Options_->AllowUndefinedResultAction) {
        SetDeny(NObjectClient::NullObjectId, NObjectClient::NullObjectId);
    }

    if (Response_.Action == NSecurityClient::ESecurityAction::Allow && Options_->Columns) {
        Response_.Columns = std::vector<TPermissionCheckResult>(Options_->Columns->size());
        std::optional<TPermissionCheckResult> deniedColumnResult;
        for (size_t index = 0; index < Options_->Columns->size(); ++index) {
            const auto& column = (*Options_->Columns)[index];
            auto& result = (*Response_.Columns)[index];
            auto it = ColumnToResult_.find(column);
            if (it == ColumnToResult_.end()) {
                result = static_cast<const TPermissionCheckResult>(Response_);
            } else {
                result = it->second;
                if (result.Action == NSecurityClient::ESecurityAction::Undefined && !FullReadExplicitlyGranted_) {
                    result.Action = NSecurityClient::ESecurityAction::Deny;
                    if (!deniedColumnResult) {
                        deniedColumnResult = result;
                    }
                }
            }
        }

        if (FullReadRequested_ && deniedColumnResult) {
            RequestedFullReadButReadIsDenied_ = false;
            SetDeny(deniedColumnResult->SubjectId, deniedColumnResult->ObjectId);
        }
    }

    if (FullReadExplicitlyGranted_) {
        // No need to mention RL ACEs if we are allowed to FullRead.
        Response_.RowLevelAcl.reset();
    }

    if (Response_.RowLevelAcl && FullReadRequested_) {
        // NB(coteeq): Presence of RL ACE alters the behaviour of non-row ACEs.
        // When RL ACEs are present, non-row allowances do not actually allow FullRead.
        // This hack is not pretty, but it exists for RL ACEs to be consistent with columnar ACEs.
        SetDeny(NObjectClient::NullObjectId, NObjectClient::NullObjectId);
    }

    Response_.RequestedFullReadButReadIsDenied = RequestedFullReadButReadIsDenied_;

    return std::move(Response_);
}

template <class TAccessControlEntry, NDetail::CSubjectMatchCallback<TAccessControlEntry> TCallback>
bool TPermissionChecker<TAccessControlEntry, TCallback>::CheckInheritanceMode(NSecurityClient::EAceInheritanceMode mode, int depth)
{
    return GetInheritedInheritanceMode(mode, depth).has_value();
}

template <class TAccessControlEntry, NDetail::CSubjectMatchCallback<TAccessControlEntry> TCallback>
bool TPermissionChecker<TAccessControlEntry, TCallback>::CheckVitalityMatch(bool vital, bool requestedVital)
{
    return !requestedVital || vital;
}

template <class TAccessControlEntry, NDetail::CSubjectMatchCallback<TAccessControlEntry> TCallback>
void TPermissionChecker<TAccessControlEntry, TCallback>::ProcessMatchingAceAction(
    TPermissionCheckResult* result,
    NSecurityClient::ESecurityAction action,
    NSecurityClient::TSubjectId subjectId,
    NObjectClient::TObjectId objectId)
{
    if (result->Action == NSecurityClient::ESecurityAction::Deny) {
        return;
    }

    result->Action = action;
    result->ObjectId = objectId;
    result->SubjectId = subjectId;
}

template <class TAccessControlEntry, NDetail::CSubjectMatchCallback<TAccessControlEntry> TCallback>
void TPermissionChecker<TAccessControlEntry, TCallback>::SetDeny(
    TPermissionCheckResult* result,
    NSecurityClient::TSubjectId subjectId,
    NObjectClient::TObjectId objectId)
{
    result->Action = NSecurityClient::ESecurityAction::Deny;
    result->SubjectId = subjectId;
    result->ObjectId = objectId;
}

template <class TAccessControlEntry, NDetail::CSubjectMatchCallback<TAccessControlEntry> TCallback>
void TPermissionChecker<TAccessControlEntry, TCallback>::SetDeny(
    NSecurityClient::TSubjectId subjectId,
    NObjectClient::TObjectId objectId)
{
    SetDeny(&Response_, subjectId, objectId);
    if (Response_.Columns) {
        for (auto& result : *Response_.Columns) {
            SetDeny(&result, subjectId, objectId);
        }
    }
    Response_.RowLevelAcl.reset();
    ShouldProceed_ = false;
}

////////////////////////////////////////////////////////////////////////////////

template <class TAccessControlEntry, NDetail::CSubjectMatchCallback<TAccessControlEntry> TCallback>
TSubtreePermissionChecker<TAccessControlEntry, TCallback>::TSubtreePermissionChecker(
    NYTree::EPermission permission,
    TCallback matchAceSubjectCallback,
    const TPermissionCheckBasicOptions* options)
    : Permission_(permission)
    , Options_(options)
    , MatchAceSubjectCallback_(std::move(matchAceSubjectCallback))
{ }

template <class TAccessControlEntry, NDetail::CSubjectMatchCallback<TAccessControlEntry> TCallback>
template <std::ranges::input_range TAccessControlEntryRange>
    requires std::same_as<std::ranges::range_value_t<TAccessControlEntryRange>, TAccessControlEntry>
void TSubtreePermissionChecker<TAccessControlEntry, TCallback>::Put(
    TAccessControlEntryRange&& acl,
    NObjectClient::TObjectId objectId,
    bool inheritAcl)
{
    if (!inheritAcl) {
        MatchingAceTrace_.push_back({
            .Entry = TBreakpoint{},
            .Depth = CurrentDepth_,
        });
    }

    for (const auto& ace : acl) {
        TrackAce(&ace, objectId);
    }

    ++CurrentDepth_;
}

template <class TAccessControlEntry, NDetail::CSubjectMatchCallback<TAccessControlEntry> TCallback>
void TSubtreePermissionChecker<TAccessControlEntry, TCallback>::Pop()
{
    --CurrentDepth_;
    while (!MatchingAceTrace_.empty() && MatchingAceTrace_.back().Depth == CurrentDepth_) {
        MatchingAceTrace_.pop_back();
    }
}

template <class TAccessControlEntry, NDetail::CSubjectMatchCallback<TAccessControlEntry> TCallback>
void TSubtreePermissionChecker<TAccessControlEntry, TCallback>::TrackAce(
    const TAccessControlEntry* ace,
    NObjectClient::TObjectId objectId)
{
    if (None(ace->Permissions & NDetail::ExtendReadPermission(Permission_))) {
        return;
    }

    auto subjectId = MatchAceSubjectCallback_(*ace);
    if (!subjectId) {
        return;
    }

    MatchingAceTrace_.push_back({
        .Entry = TMatchingAce{
            .Ace = ace,
            .ObjectId = objectId,
        },
        .Depth = CurrentDepth_,
    });
}

template <class TAccessControlEntry, NDetail::CSubjectMatchCallback<TAccessControlEntry> TCallback>
TPermissionCheckResult TSubtreePermissionChecker<TAccessControlEntry, TCallback>::CheckPermission() const
{
    using TPermissionChecker = TPermissionChecker<TAccessControlEntry, TCallback>;

    auto checker = TPermissionChecker(Permission_, MatchAceSubjectCallback_, Options_);

    for (
        auto it = MatchingAceTrace_.rbegin();
        checker.ShouldProceed() && it != MatchingAceTrace_.rend();
        ++it)
    {
        auto* entry = std::get_if<TMatchingAce>(&it->Entry);
        if (!entry) {
            break;
        }

        checker.ProcessAce(*entry->Ace, entry->ObjectId, CurrentDepth_ - 1 - it->Depth);
    }

    return std::move(checker).GetResponse();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSecurityServer
