#pragma once

#include <ytlib/misc/guid.h>

#include <ytlib/object_client/public.h>

namespace NYT {
namespace NSecurityClient {

////////////////////////////////////////////////////////////////////////////////

typedef NObjectClient::TObjectId TAccountId;
extern TAccountId NullAccountId;

extern Stroka TmpAccountName;
extern Stroka SysAccountName;

////////////////////////////////////////////////////////////////////////////////

} // namespace NSecurityClient
} // namespace NYT

