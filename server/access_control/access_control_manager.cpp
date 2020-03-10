#include "access_control_manager.h"

#include "config.h"
#include "private.h"

#include <yp/server/access_control/proto/continuation_token.pb.h>

#include <yp/server/master/bootstrap.h>
#include <yp/server/master/yt_connector.h>

#include <yp/server/objects/continuation_token.h>
#include <yp/server/objects/db_schema.h>
#include <yp/server/objects/helpers.h>
#include <yp/server/objects/object.h>
#include <yp/server/objects/object_manager.h>
#include <yp/server/objects/transaction.h>
#include <yp/server/objects/transaction_manager.h>
#include <yp/server/objects/type_handler.h>

#include <yp/server/lib/objects/object_filter.h>
#include <yp/server/lib/objects/type_info.h>

#include <yp/server/lib/query_helpers/query_evaluator.h>
#include <yp/server/lib/query_helpers/query_rewriter.h>

#include <yt/client/api/rowset.h>

#include <yt/client/table_client/helpers.h>
#include <yt/client/table_client/row_buffer.h>
#include <yt/client/table_client/schema.h>

#include <yt/core/concurrency/action_queue.h>
#include <yt/core/concurrency/fls.h>
#include <yt/core/concurrency/periodic_executor.h>
#include <yt/core/concurrency/rw_spinlock.h>
#include <yt/core/concurrency/thread_affinity.h>

#include <yt/core/misc/error.h>
#include <yt/core/misc/property.h>
#include <yt/core/misc/ref_counted.h>
#include <yt/core/misc/ref_tracked.h>

namespace NYP::NServer::NAccessControl {

using namespace NObjects;
using namespace NApi;
using namespace NRpc;
using namespace NTableClient;
using namespace NConcurrency;

using namespace NQueryClient;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

namespace {

const THashMap<NYPath::TYPath, TString> ColumnNameByAttributePathFirstToken({
    std::make_pair<NYPath::TYPath, TString>("labels", "labels"),
});

std::vector<TColumnSchema> GenerateFakeTableColumns()
{
    std::vector<TColumnSchema> columns;
    for (const auto& [attributePathFirstToken, columnName] : ColumnNameByAttributePathFirstToken) {
        columns.emplace_back(columnName, EValueType::Any);
    }
    return columns;
}

const TTableSchema& GetFakeTableSchema()
{
    static const TTableSchema schema(GenerateFakeTableColumns());
    return schema;
}

std::unique_ptr<NQueryHelpers::TQueryEvaluationContext> CreateQueryEvaluationContext(
    const std::optional<NObjects::TObjectFilter>& filter)
{
    if (!filter) {
        return nullptr;
    }

    NYT::TObjectsHolder holder;

    return std::make_unique<NQueryHelpers::TQueryEvaluationContext>(NQueryHelpers::CreateQueryEvaluationContext(
        NQueryHelpers::BuildFakeTableFilterExpression(&holder, filter->Query, ColumnNameByAttributePathFirstToken),
        GetFakeTableSchema()));
}

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

using TAcl = NObjects::TObject::TAcl;

template <class TFunction>
void ForEachAttributeAce(
    const TAcl& acl,
    const NYPath::TYPath& attributePath,
    TFunction&& function)
{
    for (const auto& ace : acl) {
        const auto& attributes = ace.attributes();
        bool shouldRun = false;
        if (attributes.empty()) {
            shouldRun = true;
        } else {
            for (const auto& attribute : attributes) {
                if (attributePath.StartsWith(attribute)) {
                    shouldRun = true;
                    break;
                }
            }
        }
        if (shouldRun) {
            function(ace);
        }
    }
}

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
    , public TRefTracked<TUser>
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
    YT_VERIFY(Type_ == EObjectType::User);
    return static_cast<TUser*>(this);
}

////////////////////////////////////////////////////////////////////////////////

class TGroup
    : public TSubject
    , public TRefTracked<TGroup>
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
    YT_VERIFY(Type_ == EObjectType::Group);
    return static_cast<TGroup*>(this);
}

////////////////////////////////////////////////////////////////////////////////

// Represents object snapshot for the access control purpose.
class TObject
    : public TRefTracked<TObject>
{
public:
    DEFINE_BYVAL_RO_PROPERTY(EObjectType, Type);
    DEFINE_BYREF_RO_PROPERTY(TObjectId, Id);
    DEFINE_BYREF_RO_PROPERTY(TObjectId, ParentId);
    DEFINE_BYREF_RO_PROPERTY(TAcl, Acl);
    DEFINE_BYVAL_RO_PROPERTY(bool, InheritAcl);
    DEFINE_BYREF_RO_PROPERTY(NYson::TYsonString, Labels);

public:
    TObject(
        EObjectType type,
        TObjectId id,
        TObjectId parentId,
        TAcl acl,
        bool inheritAcl,
        NYson::TYsonString labels)
        : Type_(type)
        , Id_(std::move(id))
        , ParentId_(std::move(parentId))
        , Acl_(std::move(acl))
        , InheritAcl_(inheritAcl)
        , Labels_(std::move(labels))
    { }
};

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
            YT_VERIFY(EveryoneGroup_->RecursiveUserIds().insert(id).second);
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
        const NYPath::TYPath& attributePath,
        EAccessControlPermission permission,
        const TObjectId& userId)
    {
        std::optional<std::tuple<EAccessControlAction, TObjectId>> result;
        ForEachAttributeAce(acl, attributePath, [&] (const auto& ace) {
            if (result && std::get<0>(*result) == EAccessControlAction::Deny) {
                return;
            }
            if (auto subresult = ApplyAce(ace, permission, userId); subresult) {
                result = std::move(subresult);
            }
        });
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
                    YT_ABORT();
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
                    YT_ABORT();
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
    void AddObjects(NObjects::EObjectType objectType, std::vector<std::unique_ptr<TObject>> objects)
    {
        YT_VERIFY(ObjectTypes_.emplace(objectType).second);
        for (auto& object : objects) {
            // NB! It is crucial to construct this argument in a separate code line
            //     to overcome UB due to unspecified order between
            //     TObject::Id call and std::unique_ptr<T> move constructor.
            auto id = object->Id();
            YT_VERIFY(Objects_[objectType].emplace(
                std::move(id),
                std::move(object)).second);
        }
        SortedObjects_[objectType].reserve(objects.size());
        for (const auto& idAndObjectPair : Objects_[objectType]) {
            SortedObjects_[objectType].push_back(idAndObjectPair.second.get());
        }
        std::sort(
            SortedObjects_[objectType].begin(),
            SortedObjects_[objectType].end(),
            [] (TObject* lhs, TObject* rhs) {
                return lhs->Id() < rhs->Id();
            });
    }

    TObject* FindObject(EObjectType type, const TObjectId& id) const
    {
        auto it = Objects_[type].find(id);
        if (it == Objects_[type].end()) {
            return nullptr;
        }
        return it->second.get();
    }

    TRange<TObject*> GetSortedObjects(EObjectType type) const
    {
        return TRange<TObject*>(SortedObjects_[type]);
    }

    bool ContainsObjectType(NObjects::EObjectType objectType) const
    {
        return ObjectTypes_.find(objectType) != ObjectTypes_.end();
    }

    void ValidateContainsObjectType(NObjects::EObjectType objectType) const
    {
        if (!ContainsObjectType(objectType)) {
            THROW_ERROR_EXCEPTION(
                NRpc::EErrorCode::NoSuchMethod,
                "Cluster access control object snapshot does not contain %v objects",
                GetHumanReadableTypeName(objectType));
        }
    }

private:
    THashSet<NObjects::EObjectType> ObjectTypes_;
    TEnumIndexedVector<EObjectType, THashMap<TObjectId, std::unique_ptr<TObject>>> Objects_;
    TEnumIndexedVector<EObjectType, std::vector<TObject*>> SortedObjects_;
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
        &ObjectsTable.Fields.Labels};
    auto* typeHandler = transaction->GetSession()->GetTypeHandler(type);
    bool hasParentIdField = false;
    if (typeHandler->GetParentType() != EObjectType::Null) {
        auto parentIdField = typeHandler->GetParentIdField();
        YT_VERIFY(parentIdField);
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
        NYson::TYsonString labels;
        TObjectId parentId;
        if (hasParentIdField) {
            FromUnversionedRow(
                row,
                &id,
                &acl,
                &inheritAcl,
                &labels,
                &parentId);
        } else {
            FromUnversionedRow(
                row,
                &id,
                &acl,
                &inheritAcl,
                &labels);
        }
        result.push_back(std::make_unique<TObject>(
            type,
            std::move(id),
            std::move(parentId),
            std::move(acl),
            inheritAcl,
            std::move(labels)));
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

// Adapts NObjects::TObject and NAccessControl::TObject interfaces.
template <class TObject>
struct TObjectTraits;

template <>
struct TObjectTraits<TObject>
{
    static bool GetInheritAcl(TObject* object)
    {
        return object->GetInheritAcl();
    }

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
    static bool GetInheritAcl(NObjects::TObject* object)
    {
        return object->InheritAcl().Load();
    }

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

class TAccessControlHierarchyBase
{
public:
    virtual ~TAccessControlHierarchyBase() = default;

    template <class TFunction>
    void ForEachImmediateParentType(IObjectTypeHandler* typeHandler, TFunction&& function) const
    {
        if (typeHandler->GetType() == EObjectType::Schema) {
            return;
        }
        if (auto parentType = typeHandler->GetParentType(); parentType != EObjectType::Null) {
            function(parentType);
        }
        function(EObjectType::Schema);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TTransactionAccessControlHierarchy
    : public TAccessControlHierarchyBase
{
public:
    template <class TFunction>
    void ForEachImmediateParent(NObjects::TObject* object, TFunction&& function) const
    {
        auto* typeHandler = object->GetTypeHandler();
        if (typeHandler->GetType() == EObjectType::Schema) {
            return;
        }
        if (auto* parent = typeHandler->GetParent(object)) {
            function(parent);
        }
        if (auto* schema = typeHandler->GetSchemaObject(object)) {
            function(schema);
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

class TSnapshotAccessControlHierarchy
    : public TAccessControlHierarchyBase
{
public:
    TSnapshotAccessControlHierarchy(
        TObjectManagerPtr objectManager,
        TClusterObjectSnapshotPtr clusterObjectSnapshot)
        : ObjectManager_(std::move(objectManager))
        , ClusterObjectSnapshot_(std::move(clusterObjectSnapshot))
    { }

    template <class TFunction>
    void ForEachImmediateParent(TObject* object, TFunction&& function) const
    {
        if (object->GetType() == EObjectType::Schema) {
            return;
        }
        auto typeHandler = ObjectManager_->GetTypeHandler(object->GetType());
        auto* parent = ClusterObjectSnapshot_->FindObject(
            typeHandler->GetParentType(),
            object->ParentId());
        if (parent) {
            function(parent);
        }
        auto* schema = ClusterObjectSnapshot_->FindObject(
            EObjectType::Schema,
            typeHandler->GetSchemaObjectId());
        if (schema) {
            function(schema);
        }
    }

private:
    const TObjectManagerPtr ObjectManager_;
    const TClusterObjectSnapshotPtr ClusterObjectSnapshot_;
};

////////////////////////////////////////////////////////////////////////////////

template <
    class TAccessControlHierarchy,
    class TObject,
    class TObjectTraits = TObjectTraits<TObject>,
    class TFunction
>
void InvokeForAccessControlHierarchy(
    const TAccessControlHierarchy& hierarchy,
    TObject* leafObject,
    TFunction&& function)
{
    if (!leafObject) {
        return;
    }
    std::queue<TObject*> objects;
    THashSet<TObject*> visitedObjects;
    auto tryPushObject = [&visitedObjects, &objects, leafObject] (TObject* object) {
        if (visitedObjects.insert(object).second) {
            objects.push(object);
        } else {
            YT_LOG_WARNING("Object is visited at least twice during access control hierarchy traversal "
                           "(ObjectType: %v, ObjectId: %v, LeafObjectType: %v, LeafObjectId: %v)",
                TObjectTraits::GetType(object),
                TObjectTraits::GetId(object),
                TObjectTraits::GetType(leafObject),
                TObjectTraits::GetId(leafObject));
        }
    };
    tryPushObject(leafObject);
    while (!objects.empty()) {
        auto* object = objects.front();
        objects.pop();
        if (!function(object)) {
            return;
        }
        if (TObjectTraits::GetInheritAcl(object)) {
            hierarchy.ForEachImmediateParent(object, tryPushObject);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

TClusterObjectSnapshotPtr BuildClusterObjectSnapshot(
    const TTransactionPtr& transaction,
    const std::vector<NObjects::EObjectType>& leafObjectTypes)
{
    THashSet<NObjects::EObjectType> visitedObjectTypes;
    {
        std::queue<EObjectType> objectTypes;
        auto tryPushObjectType = [&visitedObjectTypes, &objectTypes] (EObjectType objectType) {
            if (visitedObjectTypes.insert(objectType).second) {
                objectTypes.push(objectType);
            }
        };
        for (auto leafObjectType : leafObjectTypes) {
            if (leafObjectType != EObjectType::Null) {
                tryPushObjectType(leafObjectType);
            }
        }
        TTransactionAccessControlHierarchy hierarchy;
        while (!objectTypes.empty()) {
            auto objectType = objectTypes.front();
            objectTypes.pop();
            auto* typeHandler = transaction->GetSession()->GetTypeHandler(objectType);
            hierarchy.ForEachImmediateParentType(typeHandler, tryPushObjectType);
        }
    }

    struct TResult
    {
        NObjects::EObjectType ObjectType;
        std::vector<std::unique_ptr<TObject>> Objects;
    };

    std::vector<TFuture<std::shared_ptr<TResult>>> asyncResults;
    asyncResults.reserve(visitedObjectTypes.size());

    for (auto objectType : visitedObjectTypes) {
        asyncResults.push_back(
            BIND([=] {
                YT_LOG_DEBUG("Started loading cluster object snapshot (ObjectType: %v)",
                    objectType);
                auto objects = SelectObjects(transaction, objectType);
                auto result = std::make_shared<TResult>();
                result->ObjectType = objectType;
                result->Objects = std::move(objects);
                YT_LOG_DEBUG("Finished loading cluster object snapshot (ObjectType: %v, ObjectCount: %v)",
                    objectType,
                    result->Objects.size());
                return result;
            }).AsyncVia(GetCurrentInvoker()).Run());
    }

    auto results = WaitFor(Combine(asyncResults))
        .ValueOrThrow();

    auto snapshot = New<TClusterObjectSnapshot>();
    for (auto& result : results) {
        snapshot->AddObjects(result->ObjectType, std::move(result->Objects));
    }

    return snapshot;
}

////////////////////////////////////////////////////////////////////////////////

class TLabelFilterMatcher
{
public:
    TLabelFilterMatcher(std::optional<NObjects::TObjectFilter> filter)
        : Filter_(std::move(filter))
        , EvaluationContext_(CreateQueryEvaluationContext(Filter_))
        , RowBuffer_(New<TRowBuffer>(TRowBufferTag()))
    { }

    TErrorOr<bool> Match(const TObject* object)
    {
        if (!Filter_) {
            return true;
        }

        try {
            auto labelsValue = MakeUnversionedAnyValue(object->Labels().GetData());

            YT_VERIFY(EvaluationContext_);
            auto resultValue = NQueryHelpers::EvaluateQuery(
                *EvaluationContext_,
                &labelsValue,
                RowBuffer_.Get());

            return resultValue.Type == EValueType::Boolean && resultValue.Data.Boolean;
        } catch (const std::exception& ex) {
            return TError("Error matching %Qlv object (Id: %v)",
                object->GetType(),
                object->Id())
                << TErrorAttribute("query", Filter_->Query)
                << ex;
        }
    }

private:
    struct TRowBufferTag
    { };

    const std::optional<NObjects::TObjectFilter> Filter_;
    const std::unique_ptr<NQueryHelpers::TQueryEvaluationContext> EvaluationContext_;

    TRowBufferPtr RowBuffer_;
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
        , ClusterStateUpdateQueue_(New<TActionQueue>("AclStateUpdate"))
        , ClusterStateUpdateExecutor_(New<TPeriodicExecutor>(
            ClusterStateUpdateQueue_->GetInvoker(),
            BIND(&TImpl::OnUpdateClusterState, MakeWeak(this)),
            Config_->ClusterStateUpdatePeriod))
    {
        VERIFY_INVOKER_THREAD_AFFINITY(
            ClusterStateUpdateQueue_->GetInvoker(),
            ClusterStateUpdateThread);
    }

    void Initialize()
    {
        const auto& ytConnector = Bootstrap_->GetYTConnector();
        ytConnector->SubscribeConnected(BIND(&TImpl::OnConnected, MakeWeak(this)));
        ytConnector->SubscribeDisconnected(BIND(&TImpl::OnDisconnected, MakeWeak(this)));
    }

    TPermissionCheckResult CheckPermission(
        const TObjectId& subjectId,
        NObjects::TObject* object,
        EAccessControlPermission permission,
        const NYPath::TYPath& attributePath)
    {
        return CheckPermissionImpl(
            subjectId,
            object,
            permission,
            attributePath,
            GetClusterSubjectSnapshot(),
            TTransactionAccessControlHierarchy());
    }

    TUserIdList GetObjectAccessAllowedFor(
        NObjects::TObject* rootObject,
        EAccessControlPermission permission,
        const NYPath::TYPath& attributePath)
    {
        auto snapshot = GetClusterSubjectSnapshot();

        THashSet<TObjectId> allowedForUserIds;
        THashSet<TObjectId> deniedForUserIds;
        InvokeForAccessControlHierarchy(
            TTransactionAccessControlHierarchy(),
            rootObject,
            [&] (auto* object) {
                const auto& acl = object->Acl().Load();
                ForEachAttributeAce(acl, attributePath, [&] (const auto& ace) {
                    if (!ContainsPermission(ace, permission)) {
                        return;
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
                                YT_ABORT();
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
                                YT_ABORT();
                        }
                    }
                });
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

    TGetUserAccessAllowedToResult GetUserAccessAllowedTo(
        const NObjects::TObjectId& userId,
        NObjects::EObjectType objectType,
        EAccessControlPermission permission,
        const NYPath::TYPath& attributePath,
        const std::optional<NObjects::TObjectFilter>& filter,
        const TGetUserAccessAllowedToOptions& options)
    {
        if (options.Limit && *options.Limit < 0) {
            THROW_ERROR_EXCEPTION("Limit must be non-negative");
        }

        std::optional<NProto::TGetUserAccessAllowedToContinuationToken> continuationToken;
        if (options.ContinuationToken) {
            DeserializeContinuationToken(*options.ContinuationToken, &continuationToken.emplace());
        }

        auto clusterObjectSnapshot = GetClusterObjectSnapshot();

        clusterObjectSnapshot->ValidateContainsObjectType(objectType);
        auto objects = clusterObjectSnapshot->GetSortedObjects(objectType);

        auto snapshotAccessControlHierarchy = TSnapshotAccessControlHierarchy(
            Bootstrap_->GetObjectManager(),
            clusterObjectSnapshot);

        auto clusterSubjectSnapshot = GetClusterSubjectSnapshot();

        auto beginIt = objects.begin();
        if (continuationToken) {
            beginIt = std::upper_bound(
                objects.begin(),
                objects.end(),
                continuationToken->object_id(),
                [] (const TObjectId& upperBound, TObject* object) {
                    return upperBound < object->Id();
                });
        }

        TGetUserAccessAllowedToResult result;

        int maxObjectCount = options.Limit
            ? std::min(*options.Limit, static_cast<int>(objects.size()))
            : static_cast<int>(objects.size());
        result.ObjectIds.reserve(maxObjectCount);

        TLabelFilterMatcher filterMatcher(filter);

        for (auto it = beginIt; it != objects.end(); ++it) {
            auto* object = *it;
            if (options.Limit && static_cast<int>(result.ObjectIds.size()) >= *options.Limit) {
                break;
            }
            auto permissionCheckResult = CheckPermissionImpl(
                userId,
                object,
                permission,
                attributePath,
                clusterSubjectSnapshot,
                snapshotAccessControlHierarchy);
            if (permissionCheckResult.Action == EAccessControlAction::Allow && filterMatcher.Match(object).ValueOrThrow()) {
                result.ObjectIds.push_back(object->Id());
            }
        }

        if (!result.ObjectIds.empty()) {
            NProto::TGetUserAccessAllowedToContinuationToken newContinuationToken;
            ToProto(newContinuationToken.mutable_object_id(), result.ObjectIds.back());
            result.ContinuationToken = SerializeContinuationToken(newContinuationToken);
        }

        return result;
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

    TObjectId TryGetAuthenticatedUser()
    {
        auto userId = *AuthenticatedUserId_;
        if (!userId) {
            return {};
        }

        return *userId;
    }

    void ValidatePermission(
        NObjects::TObject* object,
        EAccessControlPermission permission,
        const NYPath::TYPath& attributePath)
    {
        YT_VERIFY(object);
        auto userId = GetAuthenticatedUser();
        auto result = CheckPermission(userId, object, permission, attributePath);
        if (result.Action == EAccessControlAction::Deny) {
            TError error;
            if (result.ObjectId && result.SubjectId) {
                error = TError(
                    NClient::NApi::EErrorCode::AuthorizationError,
                    "Access denied: %Qlv permission for %v %v is denied for %Qv by ACE at %v %v",
                    permission,
                    GetHumanReadableTypeName(object->GetType()),
                    GetObjectDisplayName(object),
                    result.SubjectId,
                    GetHumanReadableTypeName(result.ObjectType),
                    result.ObjectId);
            } else {
                error = TError(
                    NClient::NApi::EErrorCode::AuthorizationError,
                    "Access denied: %Qlv permission for %v %v is not allowed by any matching ACE",
                    permission,
                    GetHumanReadableTypeName(object->GetType()),
                    GetObjectDisplayName(object));
            }
            error.Attributes().Set("permission", permission);
            error.Attributes().Set("user_id", userId);
            error.Attributes().Set("object_type", object->GetType());
            error.Attributes().Set("object_id", object->GetId());
            error.Attributes().Set("attribute_path", attributePath);
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

    NYTree::IYPathServicePtr CreateOrchidService()
    {
        auto orchidProducer = BIND(&TImpl::BuildOrchid, MakeStrong(this));
        return NYTree::IYPathService::FromProducer(std::move(orchidProducer));
    }

private:
    NMaster::TBootstrap* const Bootstrap_;
    const TAccessControlManagerConfigPtr Config_;

    const TActionQueuePtr ClusterStateUpdateQueue_;
    const TPeriodicExecutorPtr ClusterStateUpdateExecutor_;

    TReaderWriterSpinLock ClusterSubjectSnapshotLock_;
    TClusterSubjectSnapshotPtr ClusterSubjectSnapshot_;

    TReaderWriterSpinLock ClusterObjectSnapshotLock_;
    TClusterObjectSnapshotPtr ClusterObjectSnapshot_;

    std::atomic<TTimestamp> ClusterStateTimestamp = NTransactionClient::NullTimestamp;

    static NConcurrency::TFls<std::optional<TObjectId>> AuthenticatedUserId_;

    DECLARE_THREAD_AFFINITY_SLOT(ClusterStateUpdateThread);
    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);

private:
    template <
        class TObject,
        class TObjectTraits = TObjectTraits<TObject>,
        class TAccessControlHierarchy
    >
    TPermissionCheckResult CheckPermissionImpl(
        const TObjectId& subjectId,
        TObject* rootObject,
        EAccessControlPermission permission,
        const NYPath::TYPath& attributePath,
        TClusterSubjectSnapshotPtr snapshot,
        const TAccessControlHierarchy& hierarchy)
    {
        YT_VERIFY(rootObject);

        TPermissionCheckResult result;
        result.Action = EAccessControlAction::Deny;

        if (snapshot->IsSuperuser(subjectId)) {
            result.Action = EAccessControlAction::Allow;
            return result;
        }

        InvokeForAccessControlHierarchy(
            hierarchy,
            rootObject,
            [&] (auto* object) {
                const auto& acl = TObjectTraits::GetAcl(object);
                auto subresult = snapshot->ApplyAcl(
                    acl,
                    attributePath,
                    permission,
                    subjectId);
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
                            YT_ABORT();
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
                "Cluster access control subject snapshot is not loaded yet");
        }
        return ClusterSubjectSnapshot_;
    }

    void SetClusterSubjectSnapshot(TClusterSubjectSnapshotPtr snapshot)
    {
        TWriterGuard guard(ClusterSubjectSnapshotLock_);
        std::swap(ClusterSubjectSnapshot_, snapshot);
    }

    TClusterObjectSnapshotPtr GetClusterObjectSnapshot()
    {
        TReaderGuard guard(ClusterObjectSnapshotLock_);
        if (!ClusterObjectSnapshot_) {
            THROW_ERROR_EXCEPTION(
                NRpc::EErrorCode::Unavailable,
                "Cluster access control object snapshot is not loaded yet");
        }
        return ClusterObjectSnapshot_;
    }

    void SetClusterObjectSnapshot(TClusterObjectSnapshotPtr snapshot)
    {
        TWriterGuard guard(ClusterObjectSnapshotLock_);
        std::swap(ClusterObjectSnapshot_, snapshot);
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

    void UpdateClusterSubjectSnapshot(const NObjects::TTransactionPtr& transaction)
    {
        try {
            YT_LOG_DEBUG("Started loading cluster subject snapshot");

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

    void UpdateClusterObjectSnapshot(const NObjects::TTransactionPtr& transaction)
    {
        try {
            YT_LOG_DEBUG("Started loading cluster object snapshot");

            auto snapshot = BuildClusterObjectSnapshot(
                transaction,
                Config_->ClusterStateAllowedObjectTypes);

            SetClusterObjectSnapshot(std::move(snapshot));

            YT_LOG_DEBUG("Finished loading cluster object snapshot");
        } catch (const std::exception& ex) {
            YT_LOG_WARNING(ex, "Error loading cluster object snapshot");
        }
    }

    void OnUpdateClusterState()
    {
        VERIFY_THREAD_AFFINITY(ClusterStateUpdateThread);

        try {
            YT_LOG_DEBUG("Starting cluster snapshots transaction");

            const auto& transactionManager = Bootstrap_->GetTransactionManager();
            auto transaction = WaitFor(transactionManager->StartReadOnlyTransaction())
                .ValueOrThrow();

            YT_LOG_DEBUG("Cluster snapshots transaction started (Timestamp: %llx)",
                transaction->GetStartTimestamp());

            UpdateClusterSubjectSnapshot(transaction);

            UpdateClusterObjectSnapshot(transaction);

            ClusterStateTimestamp = transaction->GetStartTimestamp();
        } catch (const std::exception& ex) {
            YT_LOG_WARNING(ex, "Error loading cluster snapshots");
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

    TTimestamp GetClusterStateTimestamp() const
    {
        return ClusterStateTimestamp;
    }

    void BuildOrchid(NYson::IYsonConsumer* consumer)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        NYTree::BuildYsonFluently(consumer)
            .BeginMap()
                .Item("cluster_state_timestamp").Value(GetClusterStateTimestamp())
            .EndMap();
    }
};

NConcurrency::TFls<std::optional<TObjectId>> TAccessControlManager::TImpl::AuthenticatedUserId_;

////////////////////////////////////////////////////////////////////////////////

TAccessControlManager::TAccessControlManager(
    NMaster::TBootstrap* bootstrap,
    TAccessControlManagerConfigPtr config)
    : Impl_(New<TImpl>(bootstrap, std::move(config)))
{ }

TAccessControlManager::~TAccessControlManager()
{ }

void TAccessControlManager::Initialize()
{
    Impl_->Initialize();
}

TPermissionCheckResult TAccessControlManager::CheckPermission(
    const TObjectId& subjectId,
    NObjects::TObject* object,
    EAccessControlPermission permission,
    const NYPath::TYPath& attributePath)
{
    return Impl_->CheckPermission(
        subjectId,
        object,
        permission,
        attributePath);
}

TUserIdList TAccessControlManager::GetObjectAccessAllowedFor(
    NObjects::TObject* object,
    EAccessControlPermission permission,
    const NYPath::TYPath& attributePath)
{
    return Impl_->GetObjectAccessAllowedFor(
        object,
        permission,
        attributePath);
}

TGetUserAccessAllowedToResult TAccessControlManager::GetUserAccessAllowedTo(
    const NObjects::TObjectId& userId,
    NObjects::EObjectType objectType,
    EAccessControlPermission permission,
    const NYPath::TYPath& attributePath,
    const std::optional<NObjects::TObjectFilter>& filter,
    const TGetUserAccessAllowedToOptions& options)
{
    return Impl_->GetUserAccessAllowedTo(
        userId,
        objectType,
        permission,
        attributePath,
        filter,
        options);
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

TObjectId TAccessControlManager::TryGetAuthenticatedUser()
{
    return Impl_->TryGetAuthenticatedUser();
}

bool TAccessControlManager::HasAuthenticatedUser()
{
    return Impl_->HasAuthenticatedUser();
}

void TAccessControlManager::ValidatePermission(
    NObjects::TObject* object,
    EAccessControlPermission permission,
    const NYPath::TYPath& attributePath)
{
    Impl_->ValidatePermission(
        object,
        permission,
        attributePath);
}

void TAccessControlManager::ValidateSuperuser(TStringBuf doWhat)
{
    return Impl_->ValidateSuperuser(doWhat);
}

NYTree::IYPathServicePtr TAccessControlManager::CreateOrchidService()
{
    return Impl_->CreateOrchidService();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NAccessControl
