#pragma once

#include <yt/yt/server/master/object_server/public.h>

#include <yt/yt/server/lib/hydra_common/public.h>

#include <yt/yt/ytlib/security_client/public.h>

#include <yt/yt/core/misc/public.h>

#include <yt/yt/core/ytree/permission.h>

namespace NYT::NSecurityServer {

////////////////////////////////////////////////////////////////////////////////

namespace NProto {

class TClusterResourceLimits;
class TClusterResources;
class TAccountStatistics;
class TRequestStatisticsUpdate;
class TUserStatistics;
class TDetailedMasterMemory;

} // namespace NProto

////////////////////////////////////////////////////////////////////////////////

using NSecurityClient::TAccountId;
using NSecurityClient::TSubjectId;
using NSecurityClient::TUserId;
using NSecurityClient::TGroupId;
using NSecurityClient::TNetworkProjectId;
using NSecurityClient::TProxyRoleId;
using NSecurityClient::TAccountResourceUsageLeaseId;

using NYTree::EPermission;
using NYTree::EPermissionSet;

using NSecurityClient::ESecurityAction;
using NSecurityClient::EAceInheritanceMode;
using NSecurityClient::EProxyKind;

using NSecurityClient::TSecurityTag;

DECLARE_ENTITY_TYPE(TAccount, TAccountId, NObjectClient::TDirectObjectIdHash)
DECLARE_ENTITY_TYPE(TAccountResourceUsageLease, TAccountResourceUsageLeaseId, NObjectClient::TDirectObjectIdHash)
DECLARE_ENTITY_TYPE(TUser, TUserId, NObjectClient::TDirectObjectIdHash)
DECLARE_ENTITY_TYPE(TGroup, TGroupId, NObjectClient::TDirectObjectIdHash)
DECLARE_ENTITY_TYPE(TNetworkProject, TNetworkProjectId, NObjectClient::TDirectObjectIdHash)
DECLARE_ENTITY_TYPE(TProxyRole, TProxyRoleId, NObjectClient::TDirectObjectIdHash)

DECLARE_MASTER_OBJECT_TYPE(TAccount)
DECLARE_MASTER_OBJECT_TYPE(TUser)

struct TAccountChunkMergerNodeTraversalsPtrContext;
template <class T>
using TAccountChunkMergerNodeTraversalsPtr = NObjectServer::TObjectPtr<T, TAccountChunkMergerNodeTraversalsPtrContext>;

DECLARE_REFCOUNTED_STRUCT(ISecurityManager)

DECLARE_REFCOUNTED_CLASS(TUserRequestLimitsOptions)
DECLARE_REFCOUNTED_CLASS(TUserQueueSizeLimitsOptions)
DECLARE_REFCOUNTED_CLASS(TUserRequestLimitsConfig)
DECLARE_REFCOUNTED_CLASS(TUserReadRequestComplexityLimitsOptions)
DECLARE_REFCOUNTED_CLASS(TSerializableUserRequestLimitsOptions)
DECLARE_REFCOUNTED_CLASS(TSerializableUserQueueSizeLimitsOptions)
DECLARE_REFCOUNTED_CLASS(TSerializableUserRequestLimitsConfig)
DECLARE_REFCOUNTED_CLASS(TSerializableUserReadRequestComplexityLimitsOptions)
class TSubject;

struct TAccessControlEntry;
struct TAccessControlList;
class TAccessControlDescriptor;

struct TAcdOverride;

struct TAccountStatistics;
using TAccountMulticellStatistics = THashMap<NObjectClient::TCellTag, TAccountStatistics>;

DECLARE_REFCOUNTED_STRUCT(TSerializableAccessControlEntry)

struct TPermissionCheckTarget;
struct TPermissionCheckOptions;
struct TPermissionCheckResult;
struct TPermissionCheckResponse;

class TClusterResources;
class TRichClusterResources;
class TClusterResourceLimits;
class TViolatedClusterResourceLimits;
struct TChunkMergerCriteria;

class TDetailedMasterMemory;

struct TUserWorkload;

constexpr int TypicalAccessLogAttributeCount = 2;
using TAttributeVector = TCompactVector<std::pair<TStringBuf, TStringBuf>, TypicalAccessLogAttributeCount>;
constexpr int TypicalSecurityTagCount = 16;
using TSecurityTagsItems = TCompactVector<TSecurityTag, TypicalSecurityTagCount>;
struct TSecurityTags;
using TInternedSecurityTags = TInternedObject<TSecurityTags>;
using TSecurityTagsRegistry = TInternRegistry<TSecurityTags>;
using TSecurityTagsRegistryPtr = TInternRegistryPtr<TSecurityTags>;

constexpr int AccountTreeDepthLimit = 10;

DECLARE_REFCOUNTED_CLASS(TSecurityManagerConfig)
DECLARE_REFCOUNTED_CLASS(TDynamicSecurityManagerConfig)

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
    (ProxyRoleCreated)
    (ProxyRoleDestroyed)
);

DEFINE_ENUM(EAccessDeniedReason,
    (DeniedByAce)
    (NoAllowingAce)
);

DEFINE_ENUM(EUserWorkloadType,
    (Read)
    (Write)
);

DEFINE_ENUM(EMasterMemoryType,
    ((Nodes)          (0))
    ((Chunks)         (1))
    ((Attributes)     (2))
    ((Tablets)        (3))
    ((Schemas)        (4))
);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSecurityServer
