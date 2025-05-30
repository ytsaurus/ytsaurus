#pragma once

#include <yt/yt/server/master/object_server/public.h>

#include <yt/yt/server/lib/hydra/public.h>

#include <yt/yt/server/lib/security_server/public.h>

#include <yt/yt/ytlib/security_client/public.h>

#include <yt/yt/core/misc/maybe_inf.h>
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

using NSecurityClient::TSecurityTag;

DECLARE_ENTITY_TYPE(TAccount, TAccountId, NObjectClient::TObjectIdEntropyHash)
DECLARE_ENTITY_TYPE(TAccountResourceUsageLease, TAccountResourceUsageLeaseId, NObjectClient::TObjectIdEntropyHash)
DECLARE_ENTITY_TYPE(TUser, TUserId, NObjectClient::TObjectIdEntropyHash)
DECLARE_ENTITY_TYPE(TGroup, TGroupId, NObjectClient::TObjectIdEntropyHash)
DECLARE_ENTITY_TYPE(TNetworkProject, TNetworkProjectId, NObjectClient::TObjectIdEntropyHash)
DECLARE_ENTITY_TYPE(TProxyRole, TProxyRoleId, NObjectClient::TObjectIdEntropyHash)

DECLARE_MASTER_OBJECT_TYPE(TAccount)
DECLARE_MASTER_OBJECT_TYPE(TAccountResourceUsageLease)
DECLARE_MASTER_OBJECT_TYPE(TSubject)
DECLARE_MASTER_OBJECT_TYPE(TUser)
DECLARE_MASTER_OBJECT_TYPE(TGroup)

struct TAccountChunkMergerNodeTraversalsPtrContext;
template <class T>
using TAccountChunkMergerNodeTraversalsPtr = NObjectServer::TObjectPtr<T, TAccountChunkMergerNodeTraversalsPtrContext>;

DECLARE_REFCOUNTED_STRUCT(ISecurityManager)

DECLARE_REFCOUNTED_STRUCT(TUserRequestLimitsOptions)
DECLARE_REFCOUNTED_STRUCT(TUserQueueSizeLimitsOptions)
DECLARE_REFCOUNTED_STRUCT(TUserRequestLimitsConfig)
DECLARE_REFCOUNTED_STRUCT(TUserReadRequestComplexityLimitsOptions)
DECLARE_REFCOUNTED_STRUCT(TSerializableUserRequestLimitsOptions)
DECLARE_REFCOUNTED_CLASS(TSerializableUserQueueSizeLimitsOptions)
DECLARE_REFCOUNTED_STRUCT(TSerializableUserRequestLimitsConfig)
DECLARE_REFCOUNTED_CLASS(TSerializableUserReadRequestComplexityLimitsOptions)
class TSubject;

struct TAccessControlEntry;
struct TAccessControlList;
class TAccessControlDescriptor;

struct TAcdOverride;

struct TAccountStatistics;
using TAccountMulticellStatistics = THashMap<NObjectClient::TCellTag, TAccountStatistics>;

DECLARE_REFCOUNTED_STRUCT(TSerializableAccessControlEntry)

struct TPermissionCheckOptions;

class TClusterResources;
class TRichClusterResources;
class TClusterResourceLimits;
class TViolatedClusterResourceLimits;
struct TChunkMergerCriteria;

class TDetailedMasterMemory;

struct TUserWorkload;

constexpr int TypicalAccessLogAttributeCount = 2;
using TAccessLogAttributes = TCompactVector<std::pair<TStringBuf, TStringBuf>, TypicalAccessLogAttributeCount>;

constexpr int TypicalSecurityTagCount = 16;
using TSecurityTagsItems = TCompactVector<TSecurityTag, TypicalSecurityTagCount>;

struct TSecurityTags;

using TInternedSecurityTags = TInternedObject<TSecurityTags>;
using TSecurityTagsRegistry = TInternRegistry<TSecurityTags>;
using TSecurityTagsRegistryPtr = TInternRegistryPtr<TSecurityTags>;

using TLimit32 = TMaybeInf<ui32>;
using TLimit64 = TMaybeInf<ui64>;

constexpr int AccountTreeDepthLimit = 10;

DECLARE_REFCOUNTED_STRUCT(TSecurityManagerConfig)
DECLARE_REFCOUNTED_STRUCT(TDynamicSecurityManagerConfig)

DECLARE_REFCOUNTED_STRUCT(IUserActivityTracker)

DECLARE_REFCOUNTED_CLASS(TRequestTracker)

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
