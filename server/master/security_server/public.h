#pragma once

#include <yt/server/lib/hydra/public.h>

#include <yt/ytlib/security_client/public.h>

#include <yt/core/misc/public.h>

#include <yt/core/ytree/permission.h>

namespace NYT::NSecurityServer {

////////////////////////////////////////////////////////////////////////////////

namespace NProto {

class TClusterResources;
class TAccountStatistics;
class TRequestStatisticsUpdate;
class TUserStatistics;

} // namespace NProto

////////////////////////////////////////////////////////////////////////////////

using NSecurityClient::TAccountId;
using NSecurityClient::TSubjectId;
using NSecurityClient::TUserId;
using NSecurityClient::TGroupId;
using NSecurityClient::TNetworkProjectId;

using NYTree::EPermission;
using NYTree::EPermissionSet;

using NSecurityClient::ESecurityAction;
using NSecurityClient::EAceInheritanceMode;

using NSecurityClient::TSecurityTag;

DECLARE_ENTITY_TYPE(TAccount, TAccountId, NObjectClient::TDirectObjectIdHash)
DECLARE_ENTITY_TYPE(TUser, TUserId, NObjectClient::TDirectObjectIdHash)
DECLARE_ENTITY_TYPE(TGroup, TGroupId, NObjectClient::TDirectObjectIdHash)
DECLARE_ENTITY_TYPE(TNetworkProject, TNetworkProjectId, NObjectClient::TDirectObjectIdHash)

DECLARE_REFCOUNTED_CLASS(TSerializableClusterResources)
DECLARE_REFCOUNTED_STRUCT(ISecurityManager)
DECLARE_REFCOUNTED_CLASS(TUserRequestLimitsOptions)
DECLARE_REFCOUNTED_CLASS(TUserRequestLimitsConfig)

class TSubject;

struct TAccessControlEntry;
struct TAccessControlList;
class TAccessControlDescriptor;

DECLARE_REFCOUNTED_STRUCT(TSerializableAccessControlEntry)

struct TPermissionCheckTarget;
struct TPermissionCheckOptions;
struct TPermissionCheckResult;
struct TPermissionCheckResponse;

struct TUserWorkload;

constexpr int TypicalAccessLogAttributeCount = 1;
using TAttributeVector = SmallVector<std::pair<TStringBuf, TStringBuf>, TypicalAccessLogAttributeCount>;
constexpr int TypicalSecurityTagCount = 16;
using TSecurityTagsItems = SmallVector<TSecurityTag, TypicalSecurityTagCount>;
struct TSecurityTags;
using TInternedSecurityTags = TInternedObject<TSecurityTags>;
using TSecurityTagsRegistry = TInternRegistry<TSecurityTags>;
using TSecurityTagsRegistryPtr = TInternRegistryPtr<TSecurityTags>;

constexpr int AccountTreeDepthLimit = 10;

DECLARE_REFCOUNTED_CLASS(TDynamicSecurityManagerConfig)

DECLARE_REFCOUNTED_CLASS(TSecurityManager)
DECLARE_REFCOUNTED_CLASS(TRequestTracker)

DEFINE_ENUM(EAccessControlEvent,
    (UserCreated)
    (GroupCreated)
    (UserDestroyed)
    (GroupDestroyed)
    (MemberAdded)
    (MemberRemoved)
    (SubjectRenamed)
    (AccessDenied)
    (ObjectAcdUpdated)
    (NetworkProjectCreated)
    (NetworkProjectDestroyed)
);

DEFINE_ENUM(EAccessDeniedReason,
    (DeniedByAce)
    (NoAllowingAce)
);

DEFINE_ENUM(EUserWorkloadType,
    (Read)
    (Write)
);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSecurityServer
