#pragma once

#include "public.h"

#include <yt/yt/core/ytree/permission.h>

#include <yt/yt/ytlib/security_client/acl.h>

#include <library/cpp/yt/logging/logger.h>

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
    //! If there are RLACEs for the object, this array contains descriptors for reader.
    std::optional<std::vector<NSecurityClient::TRowLevelAccessControlEntry>> Rlaces;
};

////////////////////////////////////////////////////////////////////////////////

class TPermissionChecker
{
public:
    TPermissionChecker(
        NYTree::EPermissionSet permissions,
        const TPermissionCheckBasicOptions& options,
        NLogging::TLogger logger);

    bool ShouldProceed() const;

    template <class TAccessControlEntry, class TCallback>
    void ProcessAce(
        const TAccessControlEntry& ace,
        const TCallback& matchAceSubjectCallback,
        NObjectClient::TObjectId objectId,
        int depth);

    TPermissionCheckResponse GetResponse();

protected:
    const bool FullReadRequested_;
    //! XXX(coteeq): May contain multiple permissions. In that case, the behaviour
    //! is kind of strange and is tied to the implementation:
    //! If we see ESecurityAction::Deny on either of specified permissions, the
    //! check is failed. Otherwise, if we have Allow on either of permissions,
    //! the check is successful.
    const NYTree::EPermissionSet PermissionsMask_;
    const TPermissionCheckBasicOptions& Options_;
    const NLogging::TLogger Logger;

    THashSet<TStringBuf> Columns_;
    THashMap<TStringBuf, TPermissionCheckResult> ColumnToResult_;

    bool Proceed_ = true;
    TPermissionCheckResponse Response_;
    bool FullReadExplicitlyGranted_ = false;

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

    bool IsOnlyReadRequested() const;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSecurityServer

#define PERMISSION_CHECKER_H
#include "permission_checker-inl.h"
#undef PERMISSION_CHECKER_H
