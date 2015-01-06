#pragma once

#include <core/misc/public.h>

#include <core/ytree/permission.h>

#include <ytlib/security_client/public.h>

namespace NYT {
namespace NSecurityServer {

////////////////////////////////////////////////////////////////////////////////

namespace NProto {

class TRequestStatisticsUpdate;

} // namespace NProto

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

class TSecurityManagerConfig;
typedef TIntrusivePtr<TSecurityManagerConfig> TSecurityManagerConfigPtr;

class TSecurityManager;
typedef TIntrusivePtr<TSecurityManager> TSecurityManagerPtr;

class TRequestTracker;
typedef TIntrusivePtr<TRequestTracker> TRequestTrackerPtr;

////////////////////////////////////////////////////////////////////////////////

} // namespace NSecurityServer
} // namespace NYT
