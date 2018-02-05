#pragma once

#include <yt/ytlib/object_client/public.h>

#include <yt/core/rpc/public.h>

namespace NYT {
namespace NSecurityClient {

////////////////////////////////////////////////////////////////////////////////

typedef NObjectClient::TObjectId TAccountId;
typedef NObjectClient::TObjectId TSubjectId;
typedef NObjectClient::TObjectId TUserId;
typedef NObjectClient::TObjectId TGroupId;

extern const TString TmpAccountName;
extern const TString SysAccountName;
extern const TString IntermediateAccountName;
extern const TString ChunkWiseAccountingMigrationAccountName;

using NRpc::RootUserName;
extern const TString GuestUserName;
extern const TString JobUserName;
extern const TString SchedulerUserName;
extern const TString FileCacheUserName;

extern const TString EveryoneGroupName;
extern const TString UsersGroupName;
extern const TString SuperusersGroupName;
extern const TString ReplicatorUserName;
extern const TString OwnerUserName;

DEFINE_ENUM(ESecurityAction,
    ((Undefined)(0))  // Intermediate state, used internally.
    ((Allow)    (1))  // Let'em go!
    ((Deny)     (2))  // No way!
);

DEFINE_ENUM(EAceInheritanceMode,
    ((ObjectAndDescendants)    (0))  // ACE applies both to the object itself and its descendants.
    ((ObjectOnly)              (1))  // ACE applies to the object only.
    ((DescendantsOnly)         (2))  // ACE applies to descendants only.
    ((ImmediateDescendantsOnly)(3))  // ACE applies to immediate (direct) descendants only.
);

DEFINE_ENUM(EErrorCode,
    ((AuthenticationError)          (900))
    ((AuthorizationError)           (901))
    ((AccountLimitExceeded)         (902))
    ((UserBanned)                   (903))
    ((RequestQueueSizeLimitExceeded)(904))
    ((NoSuchAccount)                (905))
);

////////////////////////////////////////////////////////////////////////////////

} // namespace NSecurityClient
} // namespace NYT

