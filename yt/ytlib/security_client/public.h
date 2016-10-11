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

extern const Stroka TmpAccountName;
extern const Stroka SysAccountName;
extern const Stroka IntermediateAccountName;

using NRpc::RootUserName;
extern const Stroka GuestUserName;
extern const Stroka JobUserName;
extern const Stroka SchedulerUserName;

extern const Stroka EveryoneGroupName;
extern const Stroka UsersGroupName;
extern const Stroka SuperusersGroupName;
extern const Stroka ReplicatorUserName;

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

