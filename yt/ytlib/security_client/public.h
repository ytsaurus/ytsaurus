#pragma once

#include <ytlib/misc/guid.h>

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

// TODO(babenko): move to server-side
DECLARE_ENUM(ESecurityAction,
    // Intermediate state, used internally.
    ((Undefined)(0))
    // Let'em go!
    ((Allow)(1))
    // No way!
    ((Deny)(2))
);

////////////////////////////////////////////////////////////////////////////////

} // namespace NSecurityClient
} // namespace NYT

