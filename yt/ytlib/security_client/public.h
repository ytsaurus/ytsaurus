#pragma once

#include <core/misc/guid.h>

#include <ytlib/object_client/public.h>

namespace NYT {
namespace NSecurityClient {

////////////////////////////////////////////////////////////////////////////////

typedef NObjectClient::TObjectId TAccountId;
typedef NObjectClient::TObjectId TSubjectId;
typedef NObjectClient::TObjectId TUserId;
typedef NObjectClient::TObjectId TGroupId;

extern Stroka TmpAccountName;
extern Stroka SysAccountName;

extern Stroka RootUserName;
extern Stroka GuestUserName;

extern Stroka EveryoneGroupName;
extern Stroka UsersGroupName;

DECLARE_ENUM(ESecurityAction,
    ((Undefined)(0))  // Intermediate state, used internally.
    ((Allow)    (1))  // Let'em go!
    ((Deny)     (2))  // No way!
);

DECLARE_ENUM(EErrorCode,
    ((AuthenticationError) (900))
    ((AuthorizationError)  (901))
    ((AccountIsOverLimit)  (902))
);

////////////////////////////////////////////////////////////////////////////////

} // namespace NSecurityClient
} // namespace NYT

