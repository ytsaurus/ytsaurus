#pragma once

#include <yt/yt/orm/server/objects/public.h>

#include <yt/yt/orm/client/objects/public.h>

namespace NYT::NOrm::NServer::NAccessControl {

////////////////////////////////////////////////////////////////////////////////

using NClient::NObjects::TObjectId;
using NClient::NObjects::TObjectKey;

using NClient::NObjects::TObjectTypeValues;
using NClient::NObjects::TObjectTypeValue;

////////////////////////////////////////////////////////////////////////////////

class TSnapshotSubject;
class TSnapshotUser;
class TSnapshotGroup;
DECLARE_REFCOUNTED_CLASS(TClusterSubjectSnapshot)

class TSnapshotObject;
DECLARE_REFCOUNTED_CLASS(TClusterObjectSnapshot)

struct TPermissionCheckResult;

DECLARE_REFCOUNTED_CLASS(TAccessControlManager)
DECLARE_REFCOUNTED_CLASS(TRequestTracker)

DECLARE_REFCOUNTED_CLASS(TAccessControlManagerConfig)
DECLARE_REFCOUNTED_CLASS(TRequestTrackerConfig)

DECLARE_REFCOUNTED_STRUCT(IDataModelInterop)

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EAccessControlAction,
    ((Allow)(1))
    ((Deny) (2))
);

using TAccessControlPermissionValue = int;

struct TAccessControlPermissionValues
{
    static constexpr TAccessControlPermissionValue None = 0;
    static constexpr TAccessControlPermissionValue Read = 1;
    static constexpr TAccessControlPermissionValue LimitedRead = 10;
    static constexpr TAccessControlPermissionValue Write = 2;
    static constexpr TAccessControlPermissionValue Create = 3;
    static constexpr TAccessControlPermissionValue Use = 6;
    static constexpr TAccessControlPermissionValue Administer = 9;
};

struct TAccessControlEntry
{
    EAccessControlAction Action;
    std::vector<TAccessControlPermissionValue> Permissions;
    std::vector<TObjectId> Subjects;
    std::vector<NYPath::TYPath> Attributes;
};

using TAccessControlList = std::vector<TAccessControlEntry>;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NAccessControl
