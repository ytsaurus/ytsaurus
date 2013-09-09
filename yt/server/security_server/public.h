#pragma once

#include <core/misc/common.h>

#include <core/ytree/permission.h>

#include <ytlib/security_client/public.h>

namespace NYT {
namespace NSecurityServer {

////////////////////////////////////////////////////////////////////////////////

using NSecurityClient::TAccountId;
using NSecurityClient::TSubjectId;
using NSecurityClient::TUserId;
using NSecurityClient::TGroupId;

using NYTree::EPermission;
using NYTree::EPermissionSet;

using NSecurityClient::ESecurityAction;

class TAccount;
class TSubject;
class TUser;
class TGroup;

struct TAccessControlEntry;
struct TAccessControlList;
class TAccessControlDescriptor;

struct TPermissionCheckResult;

class TSecurityManager;
typedef TIntrusivePtr<TSecurityManager> TSecurityManagerPtr;

////////////////////////////////////////////////////////////////////////////////

} // namespace NSecurityServer
} // namespace NYT
