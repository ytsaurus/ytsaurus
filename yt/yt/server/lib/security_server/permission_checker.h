#pragma once

#include "public.h"

#include <yt/yt/core/ytree/permission.h>

namespace NYT::NSecurityServer {

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

    bool Proceed_;
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
