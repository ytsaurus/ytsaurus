#pragma once

#include "public.h"

#include <yt/yt/client/security_client/acl.h>

#include <yt/yt/core/ytree/permission.h>

namespace NYT::NSecurityServer {

////////////////////////////////////////////////////////////////////////////////

//! Describes an object (or its part) for which permission check
//! was carried out.
struct TPermissionCheckTarget
{
    NObjectClient::TObjectId ObjectId;
    std::optional<std::string> Column;
};

//! Specifies additional options for permission check.
struct TPermissionCheckBasicOptions
{
    //! If given, indicates that only a subset of columns are to affected by the operation.
    std::optional<std::vector<std::string>> Columns;
    //! Should be given whenever RegisterQueueConsumer permission is checked; defined vitality
    //! of the consumer to be registered.
    std::optional<bool> Vital;
};

//! Describes the result of a permission check for a single entity.
struct TPermissionCheckResult
{
    //! Was request allowed or declined?
    //! Note that this concerns the object as a whole, even if #TPermissionCheckBasicOptions::Columns are given.
    NSecurityClient::ESecurityAction Action = NSecurityClient::ESecurityAction::Undefined;

    //! The object whose ACL contains the matching ACE.
    NObjectClient::TObjectId ObjectId = NObjectClient::NullObjectId;

    //! Subject to which the decision applies.
    NSecurityClient::TSubjectId SubjectId = NObjectClient::NullObjectId;

    //! If this flag is true and "full_read" is requested, pretend that only
    //! a regular read was requested.
    //!
    //! This flag is expected to decrease entropy with the access control rules.
    //! When the user tries to full_read a table, on which they do not even
    //! have a basic read, we should ask them to get basic read first and
    //! hope that basic read will be enough.
    //! Otherwise, if we mention full_read in the error, the user will
    //! immediately rush to get full_read, which they probably do not need.
    bool RequestedFullReadButReadIsDenied = false;
};

//! Describes the complete response of a permission check.
//! This includes the result for the principal object and also its parts (e.g. columns).
struct TPermissionCheckResponse
    : public TPermissionCheckResult
{
    //! If TPermissionCheckBasicOptions::Columns are given, this array contains
    //! results for individual columns.
    std::optional<std::vector<TPermissionCheckResult>> Columns;
    //! If there are RL ACEs for the object, this array contains descriptors for reader.
    std::optional<std::vector<NSecurityClient::TRowLevelAccessControlEntry>> RlAcl;
};

TPermissionCheckResponse MakeFastCheckPermissionResponse(
    NSecurityClient::ESecurityAction action,
    const TPermissionCheckBasicOptions& options);

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

NYTree::EPermissionSet ExtendReadPermission(NYTree::EPermissionSet original);

// Checks if the given subject matches the user or any of it's associated groups.
// The subject can be a user name, user alias, group name, or group alias.
// Returns the matched subject id, or |NullObjectId| otherwise.
template <class T, class TAccessControlEntry>
concept CSubjectMatchCallback = CInvocable<
    T,
    NSecurityClient::TSubjectId(const decltype(std::declval<TAccessControlEntry>().Subjects[0])&)>;

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

template <class TAccessControlEntry, NDetail::CSubjectMatchCallback<TAccessControlEntry> TCallback>
class TPermissionChecker
{
public:
    TPermissionChecker(
        NYTree::EPermissionSet permissions,
        TCallback matchAceSubjectCallback,
        const TPermissionCheckBasicOptions* options);

    bool ShouldProceed() const;

    void ProcessAce(
        const TAccessControlEntry& ace,
        NObjectClient::TObjectId objectId,
        int depth);

    TPermissionCheckResponse GetResponse() &&;

protected:
    const bool FullReadRequested_;
    //! XXX(coteeq): May contain multiple permissions. In that case, the behaviour
    //! is kind of strange and is tied to the implementation:
    //! If we see ESecurityAction::Deny on either of specified permissions, the
    //! check is failed. Otherwise, if we have Allow on either of permissions,
    //! the check is successful.
    const NYTree::EPermissionSet PermissionsMask_;
    const TPermissionCheckBasicOptions* Options_;

    TCallback MatchAceSubjectCallback_;

    THashSet<TStringBuf> Columns_;
    THashMap<TStringBuf, TPermissionCheckResult> ColumnToResult_;

    bool ShouldProceed_ = true;
    TPermissionCheckResponse Response_;
    bool FullReadExplicitlyGranted_ = false;
    bool RequestedFullReadButReadIsDenied_ = true;

    static bool CheckInheritanceMode(NSecurityClient::EAceInheritanceMode mode, int depth);

    static bool CheckVitalityMatch(bool vital, bool requestedVital);

    static void ProcessMatchingAceAction(
        TPermissionCheckResult* result,
        NSecurityClient::ESecurityAction action,
        NSecurityClient::TSubjectId subjectId,
        NObjectClient::TObjectId objectId);

    static void SetDeny(
        TPermissionCheckResult* result,
        NSecurityClient::TSubjectId subjectId,
        NObjectClient::TObjectId objectId);

    void SetDeny(NSecurityClient::TSubjectId subjectId, NObjectClient::TObjectId objectId);
};

////////////////////////////////////////////////////////////////////////////////

template <class TAccessControlEntry, NDetail::CSubjectMatchCallback<TAccessControlEntry> TCallback>
class TSubtreePermissionChecker
{
public:
    TSubtreePermissionChecker(
        NYTree::EPermission permission,
        TCallback matchAceSubjectCallback);

    template <std::ranges::input_range TAccessControlEntryRange>
        requires std::same_as<std::ranges::range_value_t<TAccessControlEntryRange>, TAccessControlEntry>
    void Put(
        TAccessControlEntryRange&& acl,
        NObjectClient::TObjectId objectId,
        bool inheritAcl);

    void Pop();

    TPermissionCheckResult CheckPermission() const;

protected:
    const NYTree::EPermission Permission_;

    TCallback MatchAceSubjectCallback_;
    int CurrentDepth_ = 0;

    struct TBreakpoint
    { };

    struct TMatchingAce
    {
        const TAccessControlEntry* Ace;
        NObjectClient::TObjectId ObjectId;
    };

    struct TEntry
    {
        std::variant<TBreakpoint, TMatchingAce> Entry;
        int Depth;
    };
    std::vector<TEntry> MatchingAceTrace_;

    void TrackAce(
        const TAccessControlEntry* ace,
        NObjectClient::TObjectId objectId);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSecurityServer

#define PERMISSION_CHECKER_H
#include "permission_checker-inl.h"
#undef PERMISSION_CHECKER_H
