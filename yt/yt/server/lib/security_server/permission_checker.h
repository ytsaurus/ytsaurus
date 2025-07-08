#pragma once

#include "public.h"

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
};

//! Describes the complete response of a permission check.
//! This includes the result for the principal object and also its parts (e.g. columns).
struct TPermissionCheckResponse
    : public TPermissionCheckResult
{
    //! If TPermissionCheckBasicOptions::Columns are given, this array contains
    //! results for individual columns.
    std::optional<std::vector<TPermissionCheckResult>> Columns;
};

////////////////////////////////////////////////////////////////////////////////

class TPermissionChecker
{
public:
    TPermissionChecker(
        NYTree::EPermission permission,
        const TPermissionCheckBasicOptions& options);

    bool ShouldProceed() const;

    template <class TAccessControlEntry, class TCallback>
    void ProcessAce(
        const TAccessControlEntry& ace,
        const TCallback& matchAceSubjectCallback,
        NObjectClient::TObjectId objectId,
        int depth);

    TPermissionCheckResponse GetResponse();

protected:
    const bool FullRead_;
    const NYTree::EPermission Permission_;
    const TPermissionCheckBasicOptions& Options_;

    THashSet<TStringBuf> Columns_;
    THashMap<TStringBuf, TPermissionCheckResult> ColumnToResult_;

    bool Proceed_ = true;
    TPermissionCheckResponse Response_;

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

} // namespace NYT::NSecurityServer

#define PERMISSION_CHECKER_H
#include "permission_checker-inl.h"
#undef PERMISSION_CHECKER_H
