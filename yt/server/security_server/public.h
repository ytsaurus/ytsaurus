#pragma once

#include <ytlib/misc/common.h>

#include <ytlib/security_client/public.h>

namespace NYT {
namespace NSecurityServer {

////////////////////////////////////////////////////////////////////////////////

using NSecurityClient::TAccountId;
using NSecurityClient::NullAccountId;

class TAccount;

class TSecurityManager;
typedef TIntrusivePtr<TSecurityManager> TSecurityManagerPtr;

////////////////////////////////////////////////////////////////////////////////
            
} // namespace NSecurityServer
} // namespace NYT
