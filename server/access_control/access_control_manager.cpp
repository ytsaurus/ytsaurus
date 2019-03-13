#include "access_control_manager.h"
#include "config.h"
#include "private.h"

#include <yp/server/master/bootstrap.h>
#include <yp/server/master/yt_connector.h>

#include <yp/server/objects/db_schema.h>
#include <yp/server/objects/helpers.h>
#include <yp/server/objects/object.h>
#include <yp/server/objects/object_manager.h>
#include <yp/server/objects/transaction.h>
#include <yp/server/objects/transaction_manager.h>
#include <yp/server/objects/type_handler.h>

#include <yt/client/api/rowset.h>

#include <yt/client/table_client/helpers.h>

#include <yt/ytlib/api/native/client.h>

#include <yt/core/misc/error.h>
#include <yt/core/misc/property.h>
#include <yt/core/misc/ref_tracked.h>

#include <yt/core/concurrency/rw_spinlock.h>
#include <yt/core/concurrency/periodic_executor.h>
#include <yt/core/concurrency/thread_affinity.h>
#include <yt/core/concurrency/fls.h>

namespace NYP::NServer::NAccessControl {

using namespace NObjects;
using namespace NApi;
using namespace NRpc;
using namespace NTableClient;
using namespace NConcurrency;

using TAcl = NObjects::TObject::TAcl;

////////////////////////////////////////////////////////////////////////////////

TAuthenticatedUserGuard::TAuthenticatedUserGuard(
    TAccessControlManagerPtr accessControlManager,
    const TObjectId& userId)
{
    accessControlManager->SetAuthenticatedUser(userId);
    AccessControlManager_ = std::move(accessControlManager);
}

TAuthenticatedUserGuard::TAuthenticatedUserGuard(TAuthenticatedUserGuard&& other)
{
    std::swap(AccessControlManager_, other.AccessControlManager_);
}

TAuthenticatedUserGuard::~TAuthenticatedUserGuard()
{
    if (AccessControlManager_) {
        AccessControlManager_->ResetAuthenticatedUser();
    }
}

TAuthenticatedUserGuard& TAuthenticatedUserGuard::operator=(TAuthenticatedUserGuard&& other)
{
    Release();
    std::swap(AccessControlManager_, other.AccessControlManager_);
    return *this;
}

void TAuthenticatedUserGuard::Release()
{
    if (AccessControlManager_) {
        AccessControlManager_->ResetAuthenticatedUser();
        AccessControlManager_.Reset();
    }
}

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TClusterSubjectSnapshot)
DECLARE_REFCOUNTED_CLASS(TClusterObjectSnapshot)
class TSubject;
class TUser;
class TGroup;

////////////////////////////////////////////////////////////////////////////////

class TSubject
{
public:
    DEFINE_BYVAL_RO_PROPERTY(TObjectId, Id);
    DEFINE_BYVAL_RO_PROPERTY(EObjectType, Type);

public:
    TSubject(TObjectId id, EObjectType type)
        : Id_(std::move(id))
        , Type_(type)
    { }

    virtual ~TSubject() = default;

    TUser* AsUser();
    TGroup* AsGroup();
};

////////////////////////////////////////////////////////////////////////////////

class TUser
    : public TSubject
    , public NYT::TRefTracked<TUser>
{
public:
    TUser(
        TObjectId id,
        NClient::NApi::NProto::TUserSpec spec)
        : TSubject(std::move(id), EObjectType::User)
        , Spec_(std::move(spec))
    { }

    DEFINE_BYREF_RO_PROPERTY(NClient::NApi::NProto::TUserSpec, Spec);
};

TUser* TSubject::AsUser()
{
    YCHECK(Type_ == EObjectType::User);
    return static_cast<TUser*>(this);
}

////////////////////////////////////////////////////////////////////////////////

class TGroup
    : public TSubject
    , public NYT::TRefTracked<TGroup>
{
public:
    DEFINE_BYREF_RW_PROPERTY(THashSet<TObjectId>, RecursiveUserIds);
    DEFINE_BYREF_RO_PROPERTY(NClient::NApi::NProto::TGroupSpec, Spec);

public:
    explicit TGroup(TObjectId id, NClient::NApi::NProto::TGroupSpec spec = {})
        : TSubject(std::move(id), EObjectType::Group)
        , Spec_(std::move(spec))
    { }

    bool ContainsUser(const TObjectId& userId) const
    {
        return RecursiveUserIds_.find(userId) != RecursiveUserIds_.end();
    }
};

TGroup* TSubject::AsGroup()
{
    YCHECK(Type_ == EObjectType::Group);
    return static_cast<TGroup*>(this);
}

////////////////////////////////////////////////////////////////////////////////

// Represents object snapshot for the access control purpose.
class TObject
    : public NYT::TRefTracked<TObject>
{
public:
    DEFINE_BYVAL_RO_PROPERTY(EObjectType, Type);
    DEFINE_BYREF_RO_PROPERTY(TObjectId, Id);
    DEFINE_BYREF_RO_PROPERTY(TObjectId, ParentId);
    DEFINE_BYREF_RO_PROPERTY(TAcl, Acl);
    DEFINE_BYVAL_RO_PROPERTY(bool, InheritAcl);

public:
    TObject(
        EObjectType type,
        TObjectId id,
        TObjectId parentId,
        TAcl acl,
        bool inheritAcl)
        : Type_(type)
        , Id_(std::move(id))
        , ParentId_(std::move(parentId))
        , Acl_(std::move(acl))
        , InheritAcl_(inheritAcl)
    { }
};

////////////////////////////////////////////////////////////////////////////////

namespace {

bool ContainsPermission(const NClient::NApi::NProto::TAccessControlEntry& ace, EAccessControlPermission permission)
{
    return std::find(
        ace.permissions().begin(),
        ace.permissions().end(),
        static_cast<NClient::NApi::NProto::EAccessControlPermission>(permission)) !=
        ace.permissions().end();
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

class TClusterSubjectSnapshot
    : public TRefCounted
{
public:
    TClusterSubjectSnapshot()
    {
        auto everyoneGroup = std::make_unique<TGroup>(EveryoneSubjectId);
        EveryoneGroup_ = everyoneGroup.get();
        AddSubject(std::move(everyoneGroup));
    }

    void AddSubject(std::unique_ptr<TSubject> subjectHolder)
    {
        auto* subject = subjectHolder.get();
        const auto& id = subject->GetId();
        if (!IdToSubject_.emplace(id, std::move(subjectHolder)).second) {
            THROW_ERROR_EXCEPTION("Duplicate subject %Qv",
                id);
        }

        if (subject->GetType() == EObjectType::User) {
            YCHECK(EveryoneGroup_->RecursiveUserIds().insert(id).second);
        }
    }

    bool IsSuperuser(const TObjectId& userId)
    {
        if (userId == RootUserId) {
            return true;
        }

        if (SuperusersGroup_ && SuperusersGroup_->ContainsUser(userId)) {
            return true;
        }

        return false;
    }

    TSubject* FindSubject(const TObjectId& id)
    {
        auto it = IdToSubject_.find(id);
        return it == IdToSubject_.end() ? nullptr : it->second.get();
    }

    TGroup* GetSuperusersGroup()
    {
        return SuperusersGroup_;
    }

    TGroup* GetEveryoneGroup()
    {
        return EveryoneGroup_;
    }

    void Prepare()
    {
        THashSet<TGroup*> visitedGroups;
        for (const auto& pair : IdToSubject_) {
            auto* subject = pair.second.get();
            if (subject->GetType() == EObjectType::Group) {
                auto* group = subject->AsGroup();
                visitedGroups.clear();
                ComputeRecursiveUsers(group, group, &visitedGroups);
            }
        }

        {
            auto* superusersSubject = FindSubject(SuperusersGroupId);
            if (superusersSubject) {
                if (superusersSubject->GetType() != EObjectType::Group) {
                    THROW_ERROR_EXCEPTION("%Qv must be a group",
                        SuperusersGroupId);
                }
                SuperusersGroup_ = superusersSubject->AsGroup();
            }
        }
    }

    std::optional<std::tuple<EAccessControlAction, TObjectId>> ApplyAcl(
        const std::vector<NClient::NApi::NProto::TAccessControlEntry>& acl,
        EAccessControlPermission permission,
        const TObjectId& userId)
    {
        std::optional<std::tuple<EAccessControlAction, TObjectId>> result;
        for (const auto& ace : acl) {
            auto subresult = ApplyAce(ace, permission, userId);
            if (subresult) {
                if (std::get<0>(*subresult) == EAccessControlAction::Deny) {
                    return subresult;
                }
                result = subresult;
            }
        }
        return result;
    }

private:
    THashMap<TObjectId, std::unique_ptr<TSubject>> IdToSubject_;
    TGroup* SuperusersGroup_ = nullptr;
    TGroup* EveryoneGroup_ = nullptr;

private:
    void ComputeRecursiveUsers(
        TGroup* forGroup,
        TGroup* currentGroup,
        THashSet<TGroup*>* visitedGroups)
    {
        if (!visitedGroups->insert(currentGroup).second) {
            return;
        }

        for (const auto& subjectId : currentGroup->Spec().members()) {
            auto* subject = FindSubject(subjectId);
            if (!subject) {
                continue;
            }
            switch (subject->GetType()) {
                case EObjectType::User:
                    forGroup->RecursiveUserIds().insert(subject->GetId());
                    break;
                case EObjectType::Group:
                    ComputeRecursiveUsers(forGroup, subject->AsGroup(), visitedGroups);
                    break;
                default:
                    Y_UNREACHABLE();
            }
        }
    }

    std::optional<std::tuple<EAccessControlAction, TObjectId>> ApplyAce(
        const NClient::NApi::NProto::TAccessControlEntry& ace,
        EAccessControlPermission permission,
        const TObjectId& userId)
    {
        if (!ContainsPermission(ace, permission)) {
            return std::nullopt;
        }

        for (const auto& subjectId : ace.subjects()) {
            auto* subject = FindSubject(subjectId);
            if (!subject) {
                continue;
            }

            switch (subject->GetType()) {
                case EObjectType::User:
                    if (subjectId == userId) {
                        return std::make_tuple(static_cast<EAccessControlAction>(ace.action()), subjectId);
                    }
                    break;
                case EObjectType::Group: {
                    auto* group = subject->AsGroup();
                    if (group->ContainsUser(userId)) {
                        return std::make_tuple(static_cast<EAccessControlAction>(ace.action()), subjectId);
                    }
                    break;
                }
                default:
                    Y_UNREACHABLE();
            }
        }

        return std::nullopt;
    }
};

DEFINE_REFCOUNTED_TYPE(TClusterSubjectSnapshot)

////////////////////////////////////////////////////////////////////////////////

class TClusterObjectSnapshot
    : public TRefCounted
{
public:
    void AddObjects(std::vector<std::unique_ptr<TObject>> objects)
    {
        for (auto& object : objects) {
            YCHECK(Objects_[object->GetType()].emplace(object->Id(), std::move(object)).second);
        }
    }

    TObject* FindObject(EObjectType type, const TObjectId& id) const
    {
        auto it = Objects_[type].find(id);
        if (it == Objects_[type].end()) {
            return nullptr;
        }
        return it->second.get();
    }

    std::vector<TObject*> GetObjects(EObjectType type) const
    {
        std::vector<TObject*> result;
        result.reserve(Objects_[type].size());
        for (const auto& idAndObjectPair : Objects_[type]) {
            result.push_back(idAndObjectPair.second.get());
        }
        return result;
    }

private:
    TEnumIndexedVector<THashMap<TObjectId, std::unique_ptr<TObject>>, EObjectType> Objects_;
};

DEFINE_REFCOUNTED_TYPE(TClusterObjectSnapshot)

////////////////////////////////////////////////////////////////////////////////

std::vector<std::unique_ptr<TObject>> SelectObjects(
    const TTransactionPtr& transaction,
    EObjectType type)
{
    std::vector<const TDBField*> fields{
        &ObjectsTable.Fields.Meta_Id,
        &ObjectsTable.Fields.Meta_Acl,
        &ObjectsTable.Fields.Meta_InheritAcl,
    };
    auto* typeHandler = transaction->GetSession()->GetTypeHandler(type);
    bool hasParentIdField = false;
    if (typeHandler->GetParentType() != EObjectType::Null) {
        auto parentIdField = typeHandler->GetParentIdField();
        YCHECK(parentIdField);
        fields.push_back(parentIdField);
        hasParentIdField = true;
    }
    auto rowset = transaction->SelectFields(type, fields);
    auto rows = rowset->GetRows();
    std::vector<std::unique_ptr<TObject>> result;
    result.reserve(rows.Size());
    for (auto row : rows) {
        TObjectId id;
        TAcl acl;
        bool inheritAcl;
        TObjectId parentId;
        if (hasParentIdField) {
            FromUnversionedRow(
                row,
                &id,
                &acl,
                &inheritAcl,
                &parentId);
        } else {
            FromUnversionedRow(
                row,
                &id,
                &acl,
                &inheritAcl);
        }
        result.push_back(std::make_unique<TObject>(
            type,
            std::move(id),
            std::move(parentId),
            std::move(acl),
            inheritAcl));
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

TClusterObjectSnapshotPtr BuildHierarchyObjectsSnapshot(
    const TTransactionPtr& transaction,
    EObjectType hierarchyLeafObjectType)
{
    auto snapshot = New<TClusterObjectSnapshot>();
    auto objectType = hierarchyLeafObjectType;
    while (objectType != EObjectType::Null) {
        snapshot->AddObjects(SelectObjects(transaction, objectType));
        auto* typeHandler = transaction->GetSession()->GetTypeHandler(objectType);
        objectType = GetAccessControlParentType(typeHandler);
    }
    return snapshot;
}

////////////////////////////////////////////////////////////////////////////////

struct TTransactionAccessControlHiearchy
{
    template <class TFunction>
    static void Invoke(NObjects::TObject* object, TFunction&& function)
    {
        while (object) {
            if (!function(object)) {
                break;
            }

            if (!object->InheritAcl().Load()) {
                break;
            }

            auto* typeHandler = object->GetTypeHandler();
            object = GetAccessControlParent(typeHandler, object);
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

class TSnapshotAccessControlHierarchy
{
public:
    TSnapshotAccessControlHierarchy(
        TObjectManagerPtr objectManager,
        TClusterObjectSnapshotPtr clusterObjectSnapshot)
        : ObjectManager_(std::move(objectManager))
        , ClusterObjectSnapshot_(std::move(clusterObjectSnapshot))
    { }

    template <class TFunction>
    void Invoke(TObject* object, TFunction&& function) const
    {
        while (object) {
            if (!function(object)) {
                break;
            }

            if (!object->GetInheritAcl()) {
                break;
            }

            object = GetAccessControlParent(object);
        }
    }

private:
    const TObjectManagerPtr ObjectManager_;
    const TClusterObjectSnapshotPtr ClusterObjectSnapshot_;

    TObject* GetAccessControlParent(TObject* object) const
    {
        auto typeHandler = ObjectManager_->GetTypeHandler(object->GetType());
        auto accessControlParentType = GetAccessControlParentType(typeHandler);
        if (accessControlParentType == EObjectType::Null) {
            return nullptr;
        }
        auto accessControlParentId = GetAccessControlParentId(
            typeHandler,
            object->ParentId());
        return ClusterObjectSnapshot_->FindObject(
            accessControlParentType,
            accessControlParentId);
    }
};

////////////////////////////////////////////////////////////////////////////////

// Adapts NObjects::TObject and NAccessControl::TObject interfaces.
template <class TObject>
struct TObjectTraits;

template <>
struct TObjectTraits<TObject>
{
    static const TAcl& GetAcl(TObject* object)
    {
        return object->Acl();
    }

    static const TObjectId& GetId(TObject* object)
    {
        return object->Id();
    }

    static EObjectType GetType(TObject* object)
    {
        return object->GetType();
    }
};

template <>
struct TObjectTraits<NObjects::TObject>
{
    static const TAcl& GetAcl(NObjects::TObject* object)
    {
        return object->Acl().Load();
    }

    static const TObjectId& GetId(NObjects::TObject* object)
    {
        return object->GetId();
    }

    static EObjectType GetType(NObjects::TObject* object)
    {
        return object->GetType();
    }
};

////////////////////////////////////////////////////////////////////////////////

class TAccessControlManager::TImpl
    : public TRefCounted
{
public:
    TImpl(
        NMaster::TBootstrap* bootstrap,
        TAccessControlManagerConfigPtr config)
        : Bootstrap_(bootstrap)
        , Config_(std::move(config))
        , ClusterStateUpdateExecutor_(New<TPeriodicExecutor>(
            Bootstrap_->GetControlInvoker(),
            BIND(&TImpl::OnUpdateClusterState, MakeWeak(this)),
            Config_->ClusterStateUpdatePeriod))
    { }

    void Initialize()
    {
        const auto& ytConnector = Bootstrap_->GetYTConnector();
        ytConnector->SubscribeConnected(BIND(&TImpl::OnConnected, MakeWeak(this)));
        ytConnector->SubscribeDisconnected(BIND(&TImpl::OnDisconnected, MakeWeak(this)));
    }

    TPermissionCheckResult CheckPermission(
        const TObjectId& subjectId,
        NObjects::TObject* object,
        EAccessControlPermission permission)
    {
        return CheckPermissionImpl(
            subjectId,
            object,
            permission,
            GetClusterSubjectSnapshot(),
            TTransactionAccessControlHiearchy());
    }

    TUserIdList GetObjectAccessAllowedFor(
        NObjects::TObject* object,
        EAccessControlPermission permission)
    {
        auto snapshot = GetClusterSubjectSnapshot();

        THashSet<TObjectId> allowedForUserIds;
        THashSet<TObjectId> deniedForUserIds;
        TTransactionAccessControlHiearchy::Invoke(
            object,
            [&] (auto* object) {
                const auto& acl = object->Acl().Load();
                for (const auto& ace : acl) {
                    if (!ContainsPermission(ace, permission)) {
                        continue;
                    }

                    auto handleUserId = [&] (const auto& userId) {
                        switch (ace.action()) {
                            case NClient::NApi::NProto::ACA_ALLOW:
                                allowedForUserIds.insert(userId);
                                break;
                            case NClient::NApi::NProto::ACA_DENY:
                                deniedForUserIds.insert(userId);
                                break;
                            default:
                                Y_UNREACHABLE();
                        }
                    };

                    for (const auto& subjectId : ace.subjects()) {
                        auto* subject = snapshot->FindSubject(subjectId);
                        if (!subject) {
                            continue;
                        }
                        switch (subject->GetType()) {
                            case EObjectType::User:
                                handleUserId(subjectId);
                                break;

                            case EObjectType::Group:
                                for (const auto& userId : subject->AsGroup()->RecursiveUserIds()) {
                                    handleUserId(userId);
                                }
                                break;

                            default:
                                Y_UNREACHABLE();
                        }
                    }
                }
                return true;
            });

        const auto* superusersGroup = snapshot->GetSuperusersGroup();
        if (superusersGroup) {
            for (const auto& userId : superusersGroup->RecursiveUserIds()) {
                allowedForUserIds.insert(userId);
                deniedForUserIds.erase(userId);
            }
        }

        std::vector<TObjectId> result;
        result.reserve(allowedForUserIds.size());
        for (const auto& id : allowedForUserIds) {
            if (deniedForUserIds.count(id) == 0) {
                result.push_back(id);
            }
        }

        return result;
    }

    std::vector<TObjectId> GetUserAccessAllowedTo(
        const TTransactionPtr& transaction,
        NObjects::TObject* user,
        NObjects::EObjectType objectType,
        EAccessControlPermission permission)
    {
        auto clusterObjectSnapshot = BuildHierarchyObjectsSnapshot(transaction, objectType);
        auto objects = clusterObjectSnapshot->GetObjects(objectType);
        auto snapshotAccessControlHierarchy = TSnapshotAccessControlHierarchy(
            Bootstrap_->GetObjectManager(),
            clusterObjectSnapshot);
        auto clusterSubjectSnapshot = GetClusterSubjectSnapshot();
        objects.erase(
            std::remove_if(
                objects.begin(),
                objects.end(),
                [&] (TObject* object) {
                    auto permissionCheckResult = CheckPermissionImpl(
                        user->GetId(),
                        object,
                        permission,
                        clusterSubjectSnapshot,
                        snapshotAccessControlHierarchy);
                    return permissionCheckResult.Action == EAccessControlAction::Deny;
                }),
            objects.end());
        std::vector<TObjectId> objectIds;
        objectIds.reserve(objects.size());
        for (auto* object : objects) {
            objectIds.push_back(object->Id());
        }
        return objectIds;
    }

    void SetAuthenticatedUser(const TObjectId& userId)
    {
        auto snapshot = GetClusterSubjectSnapshot();
        auto* subject = snapshot->FindSubject(userId);
        if (!subject) {
            THROW_ERROR_EXCEPTION(
                NClient::NApi::EErrorCode::AuthenticationError,
                "Authenticated user %Qv is not registered",
                userId);
        }
        if (subject->GetType() != EObjectType::User) {
            THROW_ERROR_EXCEPTION(
                NClient::NApi::EErrorCode::AuthenticationError,
                "Authenticated user %Qv is registered as %Qlv",
                userId,
                subject->GetType());
        }
        const auto* user = subject->AsUser();
        if (user->Spec().banned()) {
            THROW_ERROR_EXCEPTION(
                NClient::NApi::EErrorCode::UserBanned,
                "Authenticated user %Qv is banned",
                userId);
        }

        *AuthenticatedUserId_ = userId;
    }

    void ResetAuthenticatedUser()
    {
        AuthenticatedUserId_->reset();
    }

    bool HasAuthenticatedUser()
    {
        return static_cast<bool>(*AuthenticatedUserId_);
    }

    TObjectId GetAuthenticatedUser()
    {
        auto userId = *AuthenticatedUserId_;
        if (!userId) {
            THROW_ERROR_EXCEPTION(
                NClient::NApi::EErrorCode::AuthenticationError,
                "User is not authenticated");
        }
        return *userId;
    }

    void ValidatePermission(NObjects::TObject* object, EAccessControlPermission permission)
    {
        auto userId = GetAuthenticatedUser();
        auto result = CheckPermission(userId, object, permission);
        if (result.Action == EAccessControlAction::Deny) {
            TError error;
            if (result.ObjectId && result.SubjectId) {
                error = TError(
                    NClient::NApi::EErrorCode::AuthorizationError,
                    "Access denied: %Qlv permission for %v %v is denied for %Qv by ACE at %v %v",
                    permission,
                    GetLowercaseHumanReadableTypeName(object->GetType()),
                    GetObjectDisplayName(object),
                    result.SubjectId,
                    GetLowercaseHumanReadableTypeName(result.ObjectType),
                    result.ObjectId);
            } else {
                error = TError(
                    NClient::NApi::EErrorCode::AuthorizationError,
                    "Access denied: %Qlv permission for %v %v is not allowed by any matching ACE",
                    permission,
                    GetLowercaseHumanReadableTypeName(object->GetType()),
                    GetObjectDisplayName(object));
            }
            error.Attributes().Set("permission", permission);
            error.Attributes().Set("user_id", userId);
            error.Attributes().Set("object_type", object->GetType());
            error.Attributes().Set("object_id", object->GetId());
            if (result.ObjectId) {
                error.Attributes().Set("denied_by_id", result.ObjectId);
                error.Attributes().Set("denied_by_type", result.ObjectType);
            }
            if (result.SubjectId) {
                error.Attributes().Set("denied_for", result.SubjectId);
            }
            THROW_ERROR(error);
        }
    }

    void ValidateSuperuser(TStringBuf doWhat)
    {
        auto userId = GetAuthenticatedUser();
        auto snapshot = GetClusterSubjectSnapshot();
        if (!snapshot->IsSuperuser(userId)) {
            THROW_ERROR_EXCEPTION(
                NClient::NApi::EErrorCode::AuthorizationError,
                "User %Qv must be a superuser to %v",
                userId,
                doWhat)
                << TErrorAttribute("user_id", userId);
        }
    }

private:
    NMaster::TBootstrap* const Bootstrap_;
    const TAccessControlManagerConfigPtr Config_;

    const TPeriodicExecutorPtr ClusterStateUpdateExecutor_;

    TReaderWriterSpinLock ClusterSubjectSnapshotLock_;
    TClusterSubjectSnapshotPtr ClusterSubjectSnapshot_;

    static NConcurrency::TFls<std::optional<TObjectId>> AuthenticatedUserId_;

    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);

private:
    template <
        class TObject,
        class TObjectTraits = TObjectTraits<TObject>,
        class TAccessControlHierarchy
    >
    TPermissionCheckResult CheckPermissionImpl(
        const TObjectId& subjectId,
        TObject* object,
        EAccessControlPermission permission,
        TClusterSubjectSnapshotPtr snapshot,
        const TAccessControlHierarchy& hierarchy)
    {
        TPermissionCheckResult result;
        result.Action = EAccessControlAction::Deny;

        if (snapshot->IsSuperuser(subjectId)) {
            result.Action = EAccessControlAction::Allow;
            return result;
        }

        hierarchy.Invoke(
            object,
            [&] (auto* object) {
                const auto& acl = TObjectTraits::GetAcl(object);
                auto subresult = snapshot->ApplyAcl(acl, permission, subjectId);
                if (subresult) {
                    result.ObjectId = TObjectTraits::GetId(object);
                    result.ObjectType = TObjectTraits::GetType(object);
                    result.SubjectId = std::get<1>(*subresult);
                    switch (std::get<0>(*subresult)) {
                        case EAccessControlAction::Allow:
                            if (result.Action == EAccessControlAction::Deny) {
                                result.Action = EAccessControlAction::Allow;
                                result.SubjectId = std::get<1>(*subresult);
                            }
                            break;
                        case EAccessControlAction::Deny:
                            result.Action = EAccessControlAction::Deny;
                            return false;
                        default:
                            Y_UNREACHABLE();
                    }
                }
                return true;
            });

        return result;
    }

    TClusterSubjectSnapshotPtr GetClusterSubjectSnapshot()
    {
        TReaderGuard guard(ClusterSubjectSnapshotLock_);
        if (!ClusterSubjectSnapshot_) {
            THROW_ERROR_EXCEPTION(
                NRpc::EErrorCode::Unavailable,
                "Cluster access control state is not loaded yet");
        }
        return ClusterSubjectSnapshot_;
    }

    void SetClusterSubjectSnapshot(TClusterSubjectSnapshotPtr snapshot)
    {
        TWriterGuard guard(ClusterSubjectSnapshotLock_);
        std::swap(ClusterSubjectSnapshot_, snapshot);
    }

    void OnConnected()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        ClusterStateUpdateExecutor_->Start();
    }

    void OnDisconnected()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        ClusterStateUpdateExecutor_->Stop();
    }

    void OnUpdateClusterState()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        try {
            YT_LOG_DEBUG("Started loading cluster subject snapshot");

            YT_LOG_DEBUG("Starting cluster subject snapshot transaction");

            const auto& transactionManager = Bootstrap_->GetTransactionManager();
            auto transaction = WaitFor(transactionManager->StartReadOnlyTransaction())
                .ValueOrThrow();

            YT_LOG_DEBUG("Cluster subject snapshot transaction started (Timestamp: %llx)",
                transaction->GetStartTimestamp());

            int userCount = 0;
            int groupCount = 0;

            auto snapshot = New<TClusterSubjectSnapshot>();

            auto* session = transaction->GetSession();

            {
                session->ScheduleLoad(
                    [&] (ILoadContext* context) {
                        context->ScheduleSelect(
                            GetUserQueryString(),
                            [&] (const IUnversionedRowsetPtr& rowset) {
                                YT_LOG_DEBUG("Parsing users");
                                for (auto row : rowset->GetRows()) {
                                    ++userCount;
                                    ParseUserFromRow(snapshot, row);
                                }
                            });
                    });

                YT_LOG_DEBUG("Querying users");
                session->FlushLoads();
            }

            {
                session->ScheduleLoad(
                    [&] (ILoadContext* context) {
                        context->ScheduleSelect(
                            GetGroupQueryString(),
                            [&] (const IUnversionedRowsetPtr& rowset) {
                                YT_LOG_DEBUG("Parsing groups");
                                for (auto row : rowset->GetRows()) {
                                    ++groupCount;
                                    ParseGroupFromRow(snapshot, row);
                                }
                            });
                    });

                YT_LOG_DEBUG("Querying groups");
                session->FlushLoads();
            }

            snapshot->Prepare();
            SetClusterSubjectSnapshot(std::move(snapshot));

            YT_LOG_DEBUG("Finished loading cluster subject snapshot (UserCount: %v, GroupCount: %v)",
                userCount,
                groupCount);
        } catch (const std::exception& ex) {
            YT_LOG_WARNING(ex, "Error loading cluster subject snapshot");
        }
    }

    TString GetUserQueryString()
    {
        const auto& ytConnector = Bootstrap_->GetYTConnector();
        return Format(
            "[%v], [%v] from [%v] where is_null([%v])",
            UsersTable.Fields.Meta_Id.Name,
            UsersTable.Fields.Spec.Name,
            ytConnector->GetTablePath(&UsersTable),
            UsersTable.Fields.Meta_RemovalTime.Name);
    }

    TString GetGroupQueryString()
    {
        const auto& ytConnector = Bootstrap_->GetYTConnector();
        return Format(
            "[%v], [%v] from [%v] where is_null([%v])",
            GroupsTable.Fields.Meta_Id.Name,
            GroupsTable.Fields.Spec.Name,
            ytConnector->GetTablePath(&GroupsTable),
            GroupsTable.Fields.Meta_RemovalTime.Name);
    }

    void ParseUserFromRow(const TClusterSubjectSnapshotPtr& snapshot, TUnversionedRow row)
    {
        TObjectId userId;
        NClient::NApi::NProto::TUserSpec spec;
        FromUnversionedRow(
            row,
            &userId,
            &spec);

        auto user = std::make_unique<TUser>(std::move(userId), std::move(spec));
        snapshot->AddSubject(std::move(user));
    }

    void ParseGroupFromRow(const TClusterSubjectSnapshotPtr& snapshot, TUnversionedRow row)
    {
        TObjectId groupId;
        NClient::NApi::NProto::TGroupSpec spec;
        FromUnversionedRow(
            row,
            &groupId,
            &spec);

        auto group = std::make_unique<TGroup>(std::move(groupId), std::move(spec));
        snapshot->AddSubject(std::move(group));
    }
};

NConcurrency::TFls<std::optional<TObjectId>> TAccessControlManager::TImpl::AuthenticatedUserId_;

////////////////////////////////////////////////////////////////////////////////

TAccessControlManager::TAccessControlManager(
    NMaster::TBootstrap* bootstrap,
    TAccessControlManagerConfigPtr config)
    : Impl_(New<TImpl>(bootstrap, std::move(config)))
{ }

void TAccessControlManager::Initialize()
{
    Impl_->Initialize();
}

TPermissionCheckResult TAccessControlManager::CheckPermission(
    const TObjectId& subjectId,
    NObjects::TObject* object,
    EAccessControlPermission permission)
{
    return Impl_->CheckPermission(
        subjectId,
        object,
        permission);
}

TUserIdList TAccessControlManager::GetObjectAccessAllowedFor(
    NObjects::TObject* object,
    EAccessControlPermission permission)
{
    return Impl_->GetObjectAccessAllowedFor(
        object,
        permission);
}

std::vector<TObjectId> TAccessControlManager::GetUserAccessAllowedTo(
    const TTransactionPtr& transaction,
    NObjects::TObject* user,
    NObjects::EObjectType objectType,
    EAccessControlPermission permission)
{
    return Impl_->GetUserAccessAllowedTo(
        transaction,
        user,
        objectType,
        permission);
}

void TAccessControlManager::SetAuthenticatedUser(const TObjectId& userId)
{
    Impl_->SetAuthenticatedUser(userId);
}

void TAccessControlManager::ResetAuthenticatedUser()
{
    Impl_->ResetAuthenticatedUser();
}

TObjectId TAccessControlManager::GetAuthenticatedUser()
{
    return Impl_->GetAuthenticatedUser();
}

bool TAccessControlManager::HasAuthenticatedUser()
{
    return Impl_->HasAuthenticatedUser();
}

void TAccessControlManager::ValidatePermission(
    NObjects::TObject* object,
    EAccessControlPermission permission)
{
    Impl_->ValidatePermission(object, permission);
}

void TAccessControlManager::ValidateSuperuser(TStringBuf doWhat)
{
    return Impl_->ValidateSuperuser(doWhat);
}

////////////////////////////////////////////////////////////////////////////////

bool HasAccessControlParent(IObjectTypeHandler* typeHandler)
{
    // Assuming object is not internal (e.g. network module) or null because it has type handler.
    return typeHandler->GetType() != EObjectType::Schema;
}

EObjectType GetAccessControlParentType(IObjectTypeHandler* typeHandler)
{
    if (!HasAccessControlParent(typeHandler)) {
        return EObjectType::Null;
    }
    auto parentType = typeHandler->GetParentType();
    if (parentType != EObjectType::Null) {
        return parentType;
    }
    return EObjectType::Schema;
}

TObjectId GetAccessControlParentId(IObjectTypeHandler* typeHandler, const TObjectId& parentId)
{
    if (!HasAccessControlParent(typeHandler)) {
        return TObjectId();
    }
    if (typeHandler->GetParentType() != EObjectType::Null) {
        return parentId;
    }
    return typeHandler->GetSchemaObjectId();
}

NObjects::TObject* GetAccessControlParent(IObjectTypeHandler* typeHandler, NObjects::TObject* object)
{
    if (!HasAccessControlParent(typeHandler)) {
        return nullptr;
    }
    if (typeHandler->GetParentType() != EObjectType::Null) {
        return typeHandler->GetParent(object);
    }
    return typeHandler->GetSchemaObject(object);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NAccessControl

