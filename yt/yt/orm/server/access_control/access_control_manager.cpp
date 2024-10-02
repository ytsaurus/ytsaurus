#include "access_control_manager.h"

#include "access_control_hierarchy.h"
#include "config.h"
#include "data_model_interop.h"
#include "helpers.h"
#include "object_cluster.h"
#include "private.h"
#include "request_tracker.h"
#include "subject_cluster.h"

#include <yt/yt/orm/server/access_control/proto/continuation_token.pb.h>

#include <yt/yt/orm/server/master/bootstrap.h>
#include <yt/yt/orm/server/master/config.h>
#include <yt/yt/orm/server/master/helpers.h>
#include <yt/yt/orm/server/master/yt_connector.h>

#include <yt/yt/orm/server/objects/attribute_schema.h>
#include <yt/yt/orm/server/objects/continuation.h>
#include <yt/yt/orm/server/objects/db_schema.h>
#include <yt/yt/orm/server/objects/helpers.h>
#include <yt/yt/orm/server/objects/object.h>
#include <yt/yt/orm/server/objects/object_manager.h>
#include <yt/yt/orm/server/objects/transaction.h>
#include <yt/yt/orm/server/objects/transaction_manager.h>
#include <yt/yt/orm/server/objects/type_handler.h>

#include <yt/yt/orm/client/objects/object_filter.h>
#include <yt/yt/orm/client/objects/registry.h>
#include <yt/yt/orm/client/objects/type.h>

#include <yt/yt/orm/library/query/filter_matcher.h>

#include <yt/yt/client/api/rowset.h>
#include <yt/yt/client/table_client/row_buffer.h>

#include <yt/yt/core/ytree/fluent.h>

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/fls.h>
#include <yt/yt/core/concurrency/periodic_executor.h>
#include <yt/yt/core/concurrency/thread_affinity.h>

#include <yt/yt/core/misc/error.h>
#include <yt/yt/core/misc/property.h>
#include <yt/yt/core/misc/protobuf_helpers.h>

#include <yt/yt/core/rpc/service_detail.h>

#include <yt/yt/core/tracing/trace_context.h>

#include <library/cpp/yt/memory/atomic_intrusive_ptr.h>
#include <library/cpp/yt/memory/ref_counted.h>
#include <library/cpp/yt/memory/ref_tracked.h>

namespace NYT::NOrm::NServer::NAccessControl {

using namespace NObjects;

using namespace NClient::NObjects;

using namespace NApi;
using namespace NConcurrency;
using namespace NRpc;

using namespace NYT::NTracing;

using ::NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

namespace {

static const TString AccessControlManagerUserTag("access-control-manager");
static NConcurrency::TFlsSlot<std::optional<TAuthenticationIdentity>> AuthenticatedUserIdentitySlot;
static NConcurrency::TFlsSlot<std::string> AuthenticatedUserTicketSlot;

////////////////////////////////////////////////////////////////////////////////

// Adapts TObject and TSnapshotObject interfaces.
template <class TObject>
struct TObjectTraits;

template <>
struct TObjectTraits<TSnapshotObject>
{
    static bool GetInheritAcl(const TSnapshotObject* object, std::source_location /*location*/)
    {
        return object->GetInheritAcl();
    }

    static const TAccessControlList& GetAcl(const TSnapshotObject* object, std::source_location /*location*/)
    {
        return object->Acl();
    }

    static TObjectKey GetKey(const TSnapshotObject* object)
    {
        return object->Key();
    }

    static TObjectTypeValue GetType(const TSnapshotObject* object)
    {
        return object->GetType();
    }
};

template <>
struct TObjectTraits<NObjects::TObject>
{
    static bool GetInheritAcl(const NObjects::TObject* object, std::source_location location)
    {
        return object->InheritAcl().Load(location);
    }

    static TAccessControlList GetAcl(const NObjects::TObject* object, std::source_location location)
    {
        return object->LoadAcl(location);
    }

    static TObjectKey GetKey(const NObjects::TObject* object)
    {
        return object->GetKey();
    }

    static TObjectTypeValue GetType(const NObjects::TObject* object)
    {
        return object->GetType();
    }
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
    const TObject* leafObject,
    TFunction&& function,
    std::source_location location)
{
    if (!leafObject) {
        return;
    }
    std::queue<const TObject*> objects;
    THashSet<const TObject*> visitedObjects;
    auto tryPushObject = [&visitedObjects, &objects, leafObject] (const TObject* object) {
        if (visitedObjects.insert(object).second) {
            objects.push(object);
        } else {
            YT_LOG_WARNING("Object is visited at least twice during access control hierarchy traversal "
                "(ObjectType: %v, ObjectKey: %v, LeafObjectType: %v, LeafObjectKey: %v)",
                NClient::NObjects::GetGlobalObjectTypeRegistry()->GetTypeNameByValueOrCrash(TObjectTraits::GetType(object)),
                TObjectTraits::GetKey(object),
                NClient::NObjects::GetGlobalObjectTypeRegistry()->GetTypeNameByValueOrCrash(TObjectTraits::GetType(leafObject)),
                TObjectTraits::GetKey(leafObject));
        }
    };
    tryPushObject(leafObject);
    while (!objects.empty()) {
        auto* object = objects.front();
        objects.pop();
        if (!function(object)) {
            return;
        }
        if (TObjectTraits::GetInheritAcl(object, location)) {
            hierarchy.ForEachImmediateParent(object, tryPushObject, location);
        }
    }
}

template<
    class TAccessControlHierarchy,
    class TObject,
    class TObjectTraits = TObjectTraits<TObject>
>
void PreloadAccessControlHierarchy(
    const TAccessControlHierarchy& hierarchy,
    std::vector<const TObject*> objectsToFetch,
    std::source_location location)
{
    THashSet<const TObject*> visitedObjects;

    std::vector<const TObject*> nextLayer;
    nextLayer.reserve(objectsToFetch.size() + 1);
    objectsToFetch.reserve(objectsToFetch.size() + 1);
    while (!objectsToFetch.empty()) {
        for (const auto* object : objectsToFetch) {
            object->ScheduleAccessControlLoad();
        }

        for (auto* object : objectsToFetch) {
            if (TObjectTraits::GetInheritAcl(object, location)) {
                hierarchy.ForEachImmediateParent(object, [&] (const TObject* parent) {
                    if (visitedObjects.insert(parent).second) {
                        nextLayer.push_back(parent);
                    }
                },
                location);
            }
        }

        std::swap(nextLayer, objectsToFetch);
        nextLayer.clear();
    }
}

////////////////////////////////////////////////////////////////////////////////

class TSnapshotObjectFilterMatcher
{
public:
    TSnapshotObjectFilterMatcher(
        NMaster::IBootstrap* bootstrap,
        TObjectTypeValue objectType,
        std::optional<TObjectFilter> filter)
        : ObjectType_(objectType)
        , ObjectTypeHandler_(bootstrap->GetObjectManager()->GetTypeHandlerOrCrash(ObjectType_))
        , Filter_(std::move(filter))
        , AttributePayloadsCount_(std::invoke([&] {
            return 1 + ObjectTypeHandler_->GetIdAttributeSchemas().size() +
                ObjectTypeHandler_->GetAccessControlParentIdAttributeSchemas().size();
        }))
        , Underlying_(CreateUnderlyingFilterMatcher())
    { }

    TErrorOr<bool> Match(TSnapshotObject* object)
    {
        YT_VERIFY(object->GetType() == ObjectType_);

        auto attributePaths = BuildObjectAttributePayloads(object);
        auto resultOrError = Underlying_->Match({attributePaths.begin(), attributePaths.end()});
        if (resultOrError.IsOK()) {
            return resultOrError;
        }

        return TError("Error matching %Qv object %Qv",
            NClient::NObjects::GetGlobalObjectTypeRegistry()
                ->GetTypeNameByValueOrCrash(ObjectType_),
            object->Key())
            << TErrorAttribute("query", Filter_->Query)
            << resultOrError;
    }

private:
    const TObjectTypeValue ObjectType_;
    IObjectTypeHandler* const ObjectTypeHandler_;
    const std::optional<TObjectFilter> Filter_;
    const int AttributePayloadsCount_;
    const NQuery::IFilterMatcherPtr Underlying_;

    NQuery::IFilterMatcherPtr CreateUnderlyingFilterMatcher()
    {
        if (!Filter_) {
            return NQuery::CreateConstantFilterMatcher(true);
        }

        std::vector<NQuery::TTypedAttributePath> typedAttributePaths;
        typedAttributePaths.reserve(AttributePayloadsCount_);

        auto addTypedAttributePaths = [&] (const auto& attributeSchemas) {
            for (const auto* attribute : attributeSchemas) {
                const auto* dbField = attribute->TryGetDBField();
                YT_VERIFY(dbField);
                typedAttributePaths.push_back(NQuery::TTypedAttributePath{
                    .Path = attribute->GetPath(),
                    .Type = dbField->Type,
                });
            }
        };

        addTypedAttributePaths(ObjectTypeHandler_->GetIdAttributeSchemas());
        addTypedAttributePaths(ObjectTypeHandler_->GetAccessControlParentIdAttributeSchemas());
        typedAttributePaths.push_back(NQuery::TTypedAttributePath{
            .Path = "/labels",
            .Type = NTableClient::EValueType::Any,
        });

        YT_VERIFY(std::ssize(typedAttributePaths) == AttributePayloadsCount_);
        return NQuery::CreateFilterMatcher(
            std::move(Filter_->Query),
            std::move(typedAttributePaths));
    }

    std::vector<NYson::TYsonString> BuildObjectAttributePayloads(TSnapshotObject* object)
    {
        std::vector<NYson::TYsonString> result;
        result.reserve(AttributePayloadsCount_);

        for (const auto& keyField : object->Key()) {
            result.push_back(NYson::ConvertToYsonString(keyField));
        }
        for (const auto& keyField : object->AccessControlParentKey()) {
            result.push_back(NYson::ConvertToYsonString(keyField));
        }
        result.push_back(object->Labels());

        YT_VERIFY(std::ssize(result) == AttributePayloadsCount_);
        return result;
    }
};

} // namespace

bool HasAuthenticatedUser()
{
    return static_cast<bool>(*AuthenticatedUserIdentitySlot);
}

TAuthenticationIdentity GetAuthenticatedUserIdentity()
{
    auto identity = *AuthenticatedUserIdentitySlot;
    if (!identity) {
        THROW_ERROR_EXCEPTION(NClient::EErrorCode::AuthenticationError,
            "User is not authenticated");
    }
    return *identity;
}

std::optional<TAuthenticationIdentity> TryGetAuthenticatedUserIdentity()
{
    return *AuthenticatedUserIdentitySlot;
}

std::string TryGetAuthenticatedUserTicket()
{
    return *AuthenticatedUserTicketSlot;
}

////////////////////////////////////////////////////////////////////////////////

void FormatValue(
    TStringBuilderBase* builder,
    const TPermissionCheckResult& result,
    TStringBuf /*format*/)
{
    builder->AppendFormat("{"
        "Action: %v, "
        "ObjectKey: %v, "
        "ObjectType: %v, "
        "SubjectId: %v}",
        result.Action,
        result.ObjectKey,
        result.ObjectType,
        result.SubjectId);
}

////////////////////////////////////////////////////////////////////////////////

TAuthenticatedUserGuard::TAuthenticatedUserGuard(
    TAccessControlManagerPtr accessControlManager,
    TAuthenticationIdentity userIdentity,
    std::string userTicket)
{
    if (!userIdentity.User.empty()) {
        OldUserIdentity_ = TryGetAuthenticatedUserIdentity();
        OldUserTicket_ = TryGetAuthenticatedUserTicket();
        accessControlManager->SetAuthenticatedUser(std::move(userIdentity), std::move(userTicket));
        AccessControlManager_ = std::move(accessControlManager);
    }
}

TAuthenticatedUserGuard::TAuthenticatedUserGuard(TAuthenticatedUserGuard&& other)
{
    std::swap(AccessControlManager_, other.AccessControlManager_);
    std::swap(EnqueuedRequests_, other.EnqueuedRequests_);
}

TAuthenticatedUserGuard::~TAuthenticatedUserGuard()
{
    try {
        Release();
    } catch (const std::exception& ex) {
        YT_LOG_ERROR(ex, "Failed to release authenticated user identity");
    }
}

TAuthenticatedUserGuard& TAuthenticatedUserGuard::operator=(TAuthenticatedUserGuard&& other)
{
    Release();
    std::swap(AccessControlManager_, other.AccessControlManager_);
    std::swap(EnqueuedRequests_, other.EnqueuedRequests_);
    return *this;
}

void TAuthenticatedUserGuard::Release()
{
    if (AccessControlManager_) {
        if (EnqueuedRequests_) {
            auto user = GetAuthenticatedUserIdentity().User;
            AccessControlManager_->GetRequestTracker()->DecreaseRequestQueueSize(user, EnqueuedRequests_);
            EnqueuedRequests_ = 0;
        }
        if (OldUserIdentity_) {
            AccessControlManager_->SetAuthenticatedUser(std::move(*OldUserIdentity_), std::move(OldUserTicket_));
        } else {
            AccessControlManager_->ResetAuthenticatedUser();
        }
        AccessControlManager_.Reset();
    }
}

TFuture<void> TAuthenticatedUserGuard::ThrottleUserRequest(int requestCount, i64 requestWeight, bool soft)
{
    auto requestTracker = AccessControlManager_->GetRequestTracker();
    const auto& userId = GetAuthenticatedUserIdentity().User;
    if (requestTracker->TryIncreaseRequestQueueSize(userId, requestCount, soft)) {
        EnqueuedRequests_ += requestCount;
    } else {
        THROW_ERROR_EXCEPTION(
            NClient::EErrorCode::RequestThrottled,
            "User %Qv has exceeded its request queue size limit",
            userId);
    }
    return requestTracker->ThrottleUserRequest(userId, requestWeight, soft);
}

TAuthenticatedUserGuard MakeAuthenticatedUserGuard(
    const IServiceContextPtr& context,
    const NAccessControl::TAccessControlManagerPtr& accessControlManager)
{
    TString userTicket;
    if (context->RequestHeader().HasExtension(NRpc::NProto::TCredentialsExt::credentials_ext)) {
        userTicket = context->RequestHeader().GetExtension(NRpc::NProto::TCredentialsExt::credentials_ext)
            .user_ticket();
    }

    return TAuthenticatedUserGuard(
        accessControlManager,
        context->GetAuthenticationIdentity(),
        std::move(userTicket));
}

////////////////////////////////////////////////////////////////////////////////

class TAccessControlManager::TImpl
    : public TRefCounted
{
public:
    TImpl(
        NMaster::IBootstrap* bootstrap,
        const NYson::TProtobufEnumType* permissionsType,
        TAccessControlManagerConfigPtr config)
        : Bootstrap_(bootstrap)
        , PermissionsType_(permissionsType)
        , Config_(std::move(config))
        , Profiling_(Profiler.WithPrefix("/manager"))
        , ClusterStateUpdateQueue_(New<TActionQueue>("AclStateUpdate"))
        , ClusterStateUpdateExecutor_(New<TPeriodicExecutor>(
            ClusterStateUpdateQueue_->GetInvoker(),
            BIND(&TImpl::OnUpdateClusterState, MakeWeak(this)),
            GetConfig()->ClusterStateUpdatePeriod))
        , RequestTracker_(New<TRequestTracker>(GetConfig()->RequestTracker))
    {
        VERIFY_INVOKER_THREAD_AFFINITY(
            ClusterStateUpdateQueue_->GetInvoker(),
            ClusterStateUpdateThread);
        Bootstrap_->SubscribeConfigUpdate(BIND(&TImpl::OnConfigUpdate, MakeWeak(this)));
    }

    void Initialize()
    {
        const auto& ytConnector = Bootstrap_->GetYTConnector();
        ytConnector->SubscribeConnected(BIND(&TImpl::OnConnected, MakeWeak(this)));
        ytConnector->SubscribeDisconnected(BIND(&TImpl::OnDisconnected, MakeWeak(this)));
    }

    TClusterSubjectSnapshotPtr GetClusterSubjectSnapshot() const
    {
        auto snapshot = TryGetClusterSubjectSnapshot();
        if (!snapshot) {
            THROW_ERROR_EXCEPTION(NRpc::EErrorCode::Unavailable,
                "Cluster access control subject snapshot is not loaded yet");
        }
        return snapshot;
    }

    TClusterSubjectSnapshotPtr TryGetClusterSubjectSnapshot() const
    {
        return ClusterSubjectSnapshot_.Acquire();
    }

    bool AccessControlSnapshotsPreloaded()
    {
        return static_cast<bool>(TryGetClusterSubjectSnapshot()) &&
            static_cast<bool>(TryGetClusterObjectSnapshot());
    }

    TString FormatPermissionValue(TAccessControlPermissionValue value) const
    {
        if (PermissionsType_) {
            if (TStringBuf literal = NYson::FindProtobufEnumLiteralByValue(PermissionsType_, value)) {
                return TString(literal);
            }
        }
        return Format("EAccessControlPermission(%v)",
            value);
    }

    TAccessControlPermissionValue CheckedPermissionValueCast(int value) const
    {
        THROW_ERROR_EXCEPTION_UNLESS(NYson::FindProtobufEnumLiteralByValue(PermissionsType_, value),
            "Invalid value %v of access control permission",
            value);
        return static_cast<TAccessControlPermissionValue>(value);
    }

    TPermissionCheckResult CheckPermission(
        std::string_view subjectId,
        const NObjects::TObject* object,
        TAccessControlPermissionValue permission,
        const NYPath::TYPath& attributePath,
        std::source_location location)
    {
        return CheckPermissionImpl(
            subjectId,
            object,
            permission,
            attributePath,
            GetClusterSubjectSnapshot(),
            TTransactionAccessControlHierarchy(),
            /*checkSuperuser*/ true,
            location);
    }

    std::vector<TPermissionCheckResult> CheckPermissions(
        std::string_view subjectId,
        std::vector<TObjectPermission> permissions,
        std::source_location location)
    {
        return CheckPermissionsImpl(
            subjectId,
            std::move(permissions),
            GetClusterSubjectSnapshot(),
            TTransactionAccessControlHierarchy{},
            location);
    }

    TPermissionCheckResult CheckCachedPermission(
        std::string_view subjectId,
        NObjects::TObjectTypeValue objectType,
        const NObjects::TObjectKey& objectKey,
        TAccessControlPermissionValue permission,
        const NYPath::TYPath& attributePath)
    {
        auto clusterObjectSnapshot = GetClusterObjectSnapshot();
        clusterObjectSnapshot->ValidateContainsObjectType(objectType);

        auto* object = clusterObjectSnapshot->FindObject(objectType, objectKey);
        if (!object) {
            THROW_ERROR_EXCEPTION(NRpc::EErrorCode::Unavailable,
                "%v %Qv does not exist or was not loaded yet",
                NClient::NObjects::GetGlobalObjectTypeRegistry()->GetTypeNameByValueOrCrash(objectType),
                objectKey);
        }

        TSnapshotAccessControlHierarchy snapshotAccessControlHierarchy(
            Bootstrap_->GetObjectManager(),
            clusterObjectSnapshot);

        return CheckPermissionImpl(
            subjectId,
            object,
            permission,
            attributePath,
            GetClusterSubjectSnapshot(),
            snapshotAccessControlHierarchy);
    }

    TUserIdList GetObjectAccessAllowedFor(
        const NObjects::TObject* leafObject,
        TAccessControlPermissionValue permission,
        const NYPath::TYPath& attributePath,
        std::source_location location)
    {
        auto snapshot = GetClusterSubjectSnapshot();

        THashSet<TObjectId> allowedForUserIds;
        THashSet<TObjectId> deniedForUserIds;
        InvokeForAccessControlHierarchy(
            TTransactionAccessControlHierarchy(),
            leafObject,
            [&] (auto* object) {
                auto acl = object->LoadAcl(location);
                ForEachAttributeAce(acl, attributePath, [&] (const auto& ace) {
                    if (!ContainsPermission(ace, permission)) {
                        return;
                    }

                    auto handleUserId = [&] (const auto& userId) {
                        switch (ace.Action) {
                            case EAccessControlAction::Allow:
                                allowedForUserIds.insert(userId);
                                break;
                            case EAccessControlAction::Deny:
                                deniedForUserIds.insert(userId);
                                break;
                            default:
                                YT_ABORT();
                        }
                    };

                    for (const auto& subjectId : ace.Subjects) {
                        auto* subject = snapshot->FindSubject(subjectId);
                        if (!subject) {
                            continue;
                        }
                        switch (subject->GetType()) {
                            case TObjectTypeValues::User:
                                handleUserId(subjectId);
                                break;
                            case TObjectTypeValues::Group:
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
            },
            location);

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
        std::string_view userId,
        TObjectTypeValue objectType,
        TAccessControlPermissionValue permission,
        const NYPath::TYPath& attributePath,
        const std::optional<NObjects::TObjectFilter>& filter,
        const TGetUserAccessAllowedToOptions& options)
    {
        if (options.Limit && *options.Limit < 0) {
            THROW_ERROR_EXCEPTION("Limit must be non-negative");
        }

        TObjectKey continuationKey;
        if (options.ContinuationToken && !options.ContinuationToken->empty()) {
            NProto::TGetUserAccessAllowedToContinuationToken token;
            DeserializeContinuationToken(*options.ContinuationToken, &token);
            if (token.object_key().empty()) {
                THROW_ERROR_EXCEPTION(
                    "Broken or obsolete continuation token; retry the operation from the beginning");
            }
            // A previous version had TObjectKey = TUnversionedOwningRow. We just keep using that.
            NTableClient::TUnversionedOwningRow row;
            FromProto(&row, token.object_key());
            FromUnversionedRow(row, &continuationKey, static_cast<size_t>(row.GetCount()));
        }

        auto clusterObjectSnapshot = GetClusterObjectSnapshot();

        clusterObjectSnapshot->ValidateContainsObjectType(objectType);
        auto objects = clusterObjectSnapshot->GetSortedObjects(objectType);

        auto snapshotAccessControlHierarchy = TSnapshotAccessControlHierarchy(
            Bootstrap_->GetObjectManager(),
            clusterObjectSnapshot);

        auto clusterSubjectSnapshot = GetClusterSubjectSnapshot();

        auto beginIt = objects.begin();
        if (continuationKey) {
            beginIt = std::upper_bound(
                objects.begin(),
                objects.end(),
                continuationKey,
                [] (const TObjectKey& upperBound, auto* object) {
                    return upperBound < object->Key();
                });
        }

        TGetUserAccessAllowedToResult result;

        auto maxObjectCount = options.Limit
            ? std::min<i64>(*options.Limit, std::ssize(objects))
            : std::ssize(objects);
        result.ObjectKeys.reserve(maxObjectCount);

        TSnapshotObjectFilterMatcher filterMatcher(Bootstrap_, objectType, std::move(filter));

        for (auto it = beginIt; it != objects.end(); ++it) {
            auto* object = *it;
            if (options.Limit && std::ssize(result.ObjectKeys) >= *options.Limit) {
                break;
            }
            auto permissionCheckResult = CheckPermissionImpl(
                userId,
                object,
                permission,
                attributePath,
                clusterSubjectSnapshot,
                snapshotAccessControlHierarchy);
            if (permissionCheckResult.Action == EAccessControlAction::Allow &&
                filterMatcher.Match(object).ValueOrThrow())
            {
                result.ObjectKeys.push_back(object->Key());
            }
        }

        if (!result.ObjectKeys.empty()) {
            NProto::TGetUserAccessAllowedToContinuationToken newContinuationToken;
            auto rowBuffer = New<NTableClient::TRowBuffer>();
            auto row = ToUnversionedRow(result.ObjectKeys.back(), rowBuffer);
            ToProto(newContinuationToken.mutable_object_key(), row);
            result.ContinuationToken = SerializeContinuationToken(newContinuationToken);
        }

        return result;
    }

    void SetAuthenticatedUser(TAuthenticationIdentity identity, std::string userTicket)
    {
        auto snapshot = GetClusterSubjectSnapshot();
        auto* subject = snapshot->FindSubject(identity.User);
        if (!subject) {
            THROW_ERROR_EXCEPTION(NClient::EErrorCode::AuthenticationError,
                "Authenticated user %Qv is not registered",
                identity.User);
        }
        if (subject->GetType() != TObjectTypeValues::User) {
            THROW_ERROR_EXCEPTION(NClient::EErrorCode::AuthenticationError,
                "Authenticated user %Qv is registered as %Qv",
                identity.User,
                NClient::NObjects::GetGlobalObjectTypeRegistry()->GetTypeNameByValueOrCrash(subject->GetType()));
        }
        const auto* user = subject->AsUser();
        if (user->GetBanned()) {
            THROW_ERROR_EXCEPTION(NClient::EErrorCode::UserBanned,
                "Authenticated user %Qv is banned",
                identity.User);
        }

        *AuthenticatedUserIdentitySlot = std::move(identity);
        *AuthenticatedUserTicketSlot = std::move(userTicket);
    }

    void ResetAuthenticatedUser()
    {
        AuthenticatedUserIdentitySlot->reset();
        AuthenticatedUserTicketSlot->clear();
    }

    const TRequestTrackerPtr& GetRequestTracker() const
    {
        return RequestTracker_;
    }

    bool IsSuperuser(std::string_view userId) const
    {
        return GetClusterSubjectSnapshot()->IsSuperuser(userId);
    }

    bool ShouldPreloadAcl() const
    {
        auto userId = TryGetAuthenticatedUserIdentity().value_or(GetRootAuthenticationIdentity()).User;
        return !IsSuperuser(userId) || GetConfig()->PreloadAclForSuperusers;
    }

    TError MakePermissionError(const TPermissionCheckResult& result, std::string_view userId)
    {
        TError error;
        if (result.ObjectKey && result.SubjectId) {
            error = TError(
                NClient::EErrorCode::AuthorizationError,
                "Access denied: %Qv permission for %v %Qv is denied for %Qv by ACE at %v %Qv",
                FormatPermissionValue(result.Permission),
                NClient::NObjects::GetGlobalObjectTypeRegistry()
                    ->GetHumanReadableTypeNameOrCrash(result.OriginObjectType),
                result.OriginObjectKey,
                result.SubjectId,
                NClient::NObjects::GetGlobalObjectTypeRegistry()
                    ->GetHumanReadableTypeNameOrCrash(result.ObjectType),
                result.ObjectKey);
        } else {
            error = TError(
                NClient::EErrorCode::AuthorizationError,
                "Access denied: %Qv permission for %v %Qv is not allowed by any matching ACE",
                FormatPermissionValue(result.Permission),
                NClient::NObjects::GetGlobalObjectTypeRegistry()
                    ->GetHumanReadableTypeNameOrCrash(result.OriginObjectType),
                result.OriginObjectKey);
        }
        error.MutableAttributes()->Set("permission", FormatPermissionValue(result.Permission));
        error.MutableAttributes()->Set("user_id", userId);
        error.MutableAttributes()->Set(
            "object_type",
            NClient::NObjects::GetGlobalObjectTypeRegistry()->GetTypeNameByValueOrCrash(result.OriginObjectType));
        error.MutableAttributes()->Set("object_id", result.OriginObjectKey); // deprecated
        error.MutableAttributes()->Set("object_key", result.OriginObjectKey);
        error.MutableAttributes()->Set("attribute_path", result.AttributePath);
        if (result.ObjectKey) {
            TString serializedKey = result.ObjectKey.ToString();
            error.MutableAttributes()->Set("denied_by_id", serializedKey); // deprecated
            error.MutableAttributes()->Set("denied_by_key", serializedKey);
            error.MutableAttributes()->Set(
                "denied_by_type",
                NClient::NObjects::GetGlobalObjectTypeRegistry()->GetTypeNameByValueOrCrash(result.ObjectType));
        }
        if (result.SubjectId) {
            error.MutableAttributes()->Set("denied_for", result.SubjectId);
        }
        return error;
    }

    std::vector<TError> CollectErrors(
        const std::vector<TPermissionCheckResult>& results,
        std::string_view userId)
    {
        std::vector<TError> errors;
        for (const auto& result : results) {
            if (result.Action == EAccessControlAction::Deny) {
                errors.emplace_back(MakePermissionError(result, userId));
            }
        }
        return errors;
    }

    void ValidatePermissions(std::vector<TObjectPermission> requests, std::source_location location)
    {
        TTraceContextGuard guard(
            CreateTraceContextFromCurrent("NYT::NOrm::TAccessControlManager::ValidatePermissions"));

        auto userId = GetAuthenticatedUserIdentity().User;
        auto results = CheckPermissions(userId, std::move(requests), location);

        auto errors = CollectErrors(results, userId);
        if (!errors.empty()) {
            THROW_ERROR_EXCEPTION(NClient::EErrorCode::AuthorizationError, "Access denied") << std::move(errors);
        }
    }

    void ValidatePermission(
        const NObjects::TObject* object,
        TAccessControlPermissionValue permission,
        const NYPath::TYPath& attributePath,
        std::source_location location)
    {
        YT_VERIFY(object);
        auto userId = GetAuthenticatedUserIdentity().User;
        auto result = CheckPermission(userId, object, permission, attributePath, location);
        if (result.Action == EAccessControlAction::Deny) {
            THROW_ERROR(MakePermissionError(result, userId));
        }
    }

    void ValidateSuperuser(TStringBuf doWhat)
    {
        auto userId = GetAuthenticatedUserIdentity().User;
        auto snapshot = GetClusterSubjectSnapshot();
        if (!snapshot->IsSuperuser(userId)) {
            THROW_ERROR_EXCEPTION(
                NClient::EErrorCode::AuthorizationError,
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

    TAccessControlManagerConfigPtr GetConfig() const
    {
        return Config_.Acquire();
    }

private:
    struct TProfiling
    {
        explicit TProfiling(const NProfiling::TProfiler& profiler)
            : Time(profiler.WithPrefix("/time"))
            , Iteration(profiler.WithPrefix("/iteration"))
        { }

        struct TTime
        {
            explicit TTime(const NProfiling::TProfiler& profiler)
                : RunIteration(profiler.TimeGauge("/run_iteration"))
            { }

            NProfiling::TTimeGauge RunIteration;
        };
        const TTime Time;

        struct TIteration
        {
            explicit TIteration(const NProfiling::TProfiler& profiler)
                : Time(profiler.WithPrefix("/time"))
            { }

            struct TTime
            {
                explicit TTime(const NProfiling::TProfiler& profiler)
                    : LoadSubjects(profiler.TimeGauge("/load_subjects"))
                    , UpdateRequestTracker(profiler.TimeGauge("/update_request_tracker"))
                    , UpdateSubjectsSnapshot(profiler.TimeGauge("/update_subjects_snapshot"))
                    , UpdateObjectsSnapshot(profiler.TimeGauge("/update_objects_snapshot"))
                { }

                NProfiling::TTimeGauge LoadSubjects;
                NProfiling::TTimeGauge UpdateRequestTracker;
                NProfiling::TTimeGauge UpdateSubjectsSnapshot;
                NProfiling::TTimeGauge UpdateObjectsSnapshot;
            };
            const TTime Time;
        };
        const TIteration Iteration;
    };

    NMaster::IBootstrap* const Bootstrap_;
    const NYson::TProtobufEnumType* const PermissionsType_;
    TAtomicIntrusivePtr<TAccessControlManagerConfig> Config_;
    TProfiling Profiling_;

    const TActionQueuePtr ClusterStateUpdateQueue_;
    const TPeriodicExecutorPtr ClusterStateUpdateExecutor_;
    const TRequestTrackerPtr RequestTracker_;

    TAtomicIntrusivePtr<TClusterSubjectSnapshot> ClusterSubjectSnapshot_;

    TAtomicIntrusivePtr<TClusterObjectSnapshot> ClusterObjectSnapshot_;

    std::atomic<TTimestamp> ClusterStateTimestamp_ = NTransactionClient::NullTimestamp;

    DECLARE_THREAD_AFFINITY_SLOT(ClusterStateUpdateThread);
    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);

    void OnConfigUpdate(const NMaster::TMasterDynamicConfigPtr& masterConfig)
    {
        auto config = masterConfig->GetAccessControlManagerConfig();
        if (NMaster::AreConfigsEqual(config, GetConfig())) {
            return;
        }
        YT_LOG_INFO("Updating access control manager configuration");
        Config_.Store(std::move(config));
    }

    template <
        class TObject,
        class TObjectTraits = TObjectTraits<TObject>,
        class TAccessControlHierarchy
    >
    TPermissionCheckResult CheckPermissionImpl(
        std::string_view subjectId,
        const TObject* leafObject,
        TAccessControlPermissionValue permission,
        const NYPath::TYPath& attributePath,
        TClusterSubjectSnapshotPtr snapshot,
        const TAccessControlHierarchy& hierarchy,
        bool checkSuperuser = true,
        std::source_location location = std::source_location::current())
    {
        YT_VERIFY(leafObject);

        TPermissionCheckResult result;
        result.Action = EAccessControlAction::Deny;
        result.Permission = permission;
        result.AttributePath = attributePath;
        result.OriginObjectKey = TObjectTraits::GetKey(leafObject);
        result.OriginObjectType = TObjectTraits::GetType(leafObject);

        if (checkSuperuser && snapshot->IsSuperuser(subjectId)) {
            result.Action = EAccessControlAction::Allow;
            YT_LOG_DEBUG("Subject is superuser, allowing permission ("
                "SubjectId: %v, Permission: %v, ObjectKey: %v, Path: %v)",
                subjectId,
                FormatPermissionValue(permission),
                TObjectTraits::GetKey(leafObject),
                attributePath);
            return result;
        }

        InvokeForAccessControlHierarchy(
            hierarchy,
            leafObject,
            [&] (auto* object) {
                const auto& acl = TObjectTraits::GetAcl(object, location);
                auto subresult = snapshot->ApplyAcl(
                    acl,
                    attributePath,
                    permission,
                    subjectId);
                if (subresult) {
                    result.ObjectKey = TObjectTraits::GetKey(object);
                    result.ObjectType = TObjectTraits::GetType(object);
                    result.SubjectId = std::get<1>(*subresult);
                    switch (std::get<0>(*subresult)) {
                        case EAccessControlAction::Allow:
                            if (result.Action == EAccessControlAction::Deny) {
                                result.Action = EAccessControlAction::Allow;
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
            },
            location);
        YT_LOG_DEBUG("Checked object permission (PermissionCheckResult: %v, Permission: %v)",
            result,
            FormatPermissionValue(permission));

        return result;
    }

    template<class TAccessControlHierarchy>
    std::vector<TPermissionCheckResult> CheckPermissionsImpl(
        std::string_view subjectId,
        std::vector<TObjectPermission> objectPermissions,
        TClusterSubjectSnapshotPtr snapshot,
        const TAccessControlHierarchy& hierarchy,
        std::source_location location)
    {
        std::vector<TPermissionCheckResult> results;
        results.reserve(objectPermissions.size());

        if (snapshot->IsSuperuser(subjectId)) {
            return std::vector(objectPermissions.size(), TPermissionCheckResult{.Action = EAccessControlAction::Allow});
        }

        std::vector<const TObject*> objects;
        objects.reserve(objectPermissions.size());
        for (const auto& objectPermission : objectPermissions) {
            objects.push_back(objectPermission.Object);
        }
        PreloadAccessControlHierarchy(hierarchy, std::move(objects), location);

        for (const auto& objectPermission : objectPermissions) {
            auto result = CheckPermissionImpl(
                subjectId,
                objectPermission.Object,
                objectPermission.Permission,
                objectPermission.AttributePath,
                snapshot,
                hierarchy,
                /*checkSuperuser*/ false,
                location);
            results.push_back(std::move(result));
        }

        return results;
    }

    void SetClusterSubjectSnapshot(TClusterSubjectSnapshotPtr snapshot)
    {
        ClusterSubjectSnapshot_.Store(std::move(snapshot));
    }

    TClusterObjectSnapshotPtr GetClusterObjectSnapshot()
    {
        auto snapshot = TryGetClusterObjectSnapshot();
        if (!snapshot) {
            THROW_ERROR_EXCEPTION(
                NRpc::EErrorCode::Unavailable,
                "Cluster access control object snapshot is not loaded yet");
        }
        return snapshot;
    }

    TClusterObjectSnapshotPtr TryGetClusterObjectSnapshot()
    {
        return ClusterObjectSnapshot_.Acquire();
    }

    void SetClusterObjectSnapshot(TClusterObjectSnapshotPtr snapshot)
    {
        ClusterObjectSnapshot_.Store(std::move(snapshot));
    }

    void OnConnected()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        ClusterStateUpdateExecutor_->Start();
    }

    void OnDisconnected()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        YT_UNUSED_FUTURE(ClusterStateUpdateExecutor_->Stop());
    }

    void UpdateClusterSubjectSnapshot(const NObjects::TTransactionPtr& transaction)
    {
        try {
            YT_LOG_DEBUG("Started loading cluster subject snapshot");

            auto ytConnector = Bootstrap_->GetYTConnector();
            auto dataModelInterop = Bootstrap_->GetAccessControlDataModelInterop();

            int userCount = 0;
            int groupCount = 0;

            auto snapshot = New<TClusterSubjectSnapshot>();

            auto* session = transaction->GetSession();

            {
                NProfiling::TEventTimerGuard timer(Profiling_.Iteration.Time.LoadSubjects);
                session->ScheduleLoad(
                    [&] (ILoadContext* context) {
                        context->ScheduleSelect(
                            dataModelInterop->GetUserQueryString(ytConnector),
                            [&] (const NYT::NApi::IUnversionedRowsetPtr& rowset) {
                                YT_LOG_DEBUG("Parsing users");
                                for (auto row : rowset->GetRows()) {
                                    ++userCount;
                                    snapshot->AddSubject(dataModelInterop->ParseUser(row));
                                }
                            });
                    });

                session->ScheduleLoad(
                    [&] (ILoadContext* context) {
                        context->ScheduleSelect(
                            dataModelInterop->GetGroupQueryString(ytConnector),
                            [&] (const NYT::NApi::IUnversionedRowsetPtr& rowset) {
                                YT_LOG_DEBUG("Parsing groups");
                                for (auto row : rowset->GetRows()) {
                                    ++groupCount;
                                    snapshot->AddSubject(dataModelInterop->ParseGroup(row));
                                }
                            });
                    });

                session->FlushLoads();
            }

            snapshot->Prepare();

            {
                NProfiling::TEventTimerGuard timer(Profiling_.Iteration.Time.UpdateRequestTracker);
                auto oldSnapshot = TryGetClusterSubjectSnapshot();
                UpdateRequestTrackerState(oldSnapshot, snapshot);
            }

            SetClusterSubjectSnapshot(std::move(snapshot));

            YT_LOG_DEBUG("Finished loading cluster subject snapshot (UserCount: %v, GroupCount: %v)",
                userCount,
                groupCount);
        } catch (const std::exception& ex) {
            YT_LOG_WARNING(ex, "Error loading cluster subject snapshot");
        }
    }

    void UpdateRequestTrackerState(
        const TClusterSubjectSnapshotPtr& oldSnapshot,
        const TClusterSubjectSnapshotPtr& newSnapshot)
    {
        YT_LOG_INFO("Started reconciling request tracker state");

        std::vector<TRequestTracker::TUserSetup> batch;

        auto defaultUserRequestWeightRateLimit = GetConfig()->RequestTracker->DefaultUserRequestWeightRateLimit;
        auto defaultUserRequestQueueSizeLimit = GetConfig()->RequestTracker->DefaultUserRequestQueueSizeLimit;

        auto getWeightRateLimit = [defaultUserRequestWeightRateLimit] (auto* subject) {
            return subject->AsUser()->GetRequestWeightRateLimit()
                .value_or(defaultUserRequestWeightRateLimit);
        };

        auto getQueueSizeLimit = [defaultUserRequestQueueSizeLimit] (auto* subject) {
            return subject->AsUser()->GetRequestQueueSizeLimit()
                .value_or(defaultUserRequestQueueSizeLimit);
        };

        /*
         * NB: obsolete users deletion is not supported. Throttlers reside in memory, thus will be discarded
         * upon next ORM master restart.
         */
        for (const auto& newSubject : newSnapshot->GetSubjects(TObjectTypeValues::User)) {
            auto newRequestRate = getWeightRateLimit(newSubject);
            auto newRequestQueueSizeLimit = getQueueSizeLimit(newSubject);

            if (!oldSnapshot) {
                batch.emplace_back(
                    newSubject->GetId(),
                    newRequestRate,
                    newRequestQueueSizeLimit);
                continue;
            }

            auto* oldSubject = oldSnapshot->FindSubject(newSubject->GetId());
            if (!oldSubject) {
                batch.emplace_back(
                    newSubject->GetId(),
                    newRequestRate,
                    newRequestQueueSizeLimit);
                continue;
            }

            auto oldRequestRate = getWeightRateLimit(oldSubject);
            auto oldRequestQueueSizeLimit = getQueueSizeLimit(oldSubject);

            if (newRequestRate != oldRequestRate || newRequestQueueSizeLimit != oldRequestQueueSizeLimit) {
                batch.emplace_back(
                    newSubject->GetId(),
                    newRequestRate,
                    newRequestQueueSizeLimit);
            }

        }
        RequestTracker_->ReconfigureUsersBatch(batch);
        YT_LOG_INFO("Finished reconciling request tracker state (UpdateCount: %v)", batch.size());
    }

    void UpdateClusterObjectSnapshot(const NObjects::TTransactionPtr& transaction)
    {
        try {
            YT_LOG_DEBUG("Started loading cluster object snapshot");

            auto snapshot = BuildClusterObjectSnapshot(
                Bootstrap_->GetAccessControlDataModelInterop(),
                transaction,
                GetConfig()->GetClusterStateAllowedObjectTypes());

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
            NProfiling::TEventTimerGuard timer(Profiling_.Time.RunIteration);

            const auto& transactionManager = Bootstrap_->GetTransactionManager();
            auto transaction = WaitFor(transactionManager->StartReadOnlyTransaction(
                {
                    .UserTag = AccessControlManagerUserTag,
                    .ReadingTransactionOptions{
                        .AllowFullScan = true,
                    }
                }))
                .ValueOrThrow();

            YT_LOG_DEBUG("Cluster snapshots transaction started (Timestamp: %v)",
                transaction->GetStartTimestamp());

            {
                NProfiling::TEventTimerGuard timer(Profiling_.Iteration.Time.UpdateSubjectsSnapshot);
                UpdateClusterSubjectSnapshot(transaction);
            }

            {
                NProfiling::TEventTimerGuard timer(Profiling_.Iteration.Time.UpdateObjectsSnapshot);
                UpdateClusterObjectSnapshot(transaction);
            }

            ClusterStateTimestamp_ = transaction->GetStartTimestamp();
        } catch (const std::exception& ex) {
            YT_LOG_WARNING(ex, "Error loading cluster snapshots");
        }
    }

    TTimestamp GetClusterStateTimestamp() const
    {
        return ClusterStateTimestamp_.load(std::memory_order::relaxed);
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

////////////////////////////////////////////////////////////////////////////////

TAccessControlManager::TAccessControlManager(
    NMaster::IBootstrap* bootstrap,
    const NYson::TProtobufEnumType* permissionsType,
    TAccessControlManagerConfigPtr config)
    : Impl_(New<TImpl>(bootstrap, permissionsType, std::move(config)))
{ }

TAccessControlManager::~TAccessControlManager()
{ }

void TAccessControlManager::Initialize()
{
    Impl_->Initialize();
}

bool TAccessControlManager::AccessControlSnapshotsPreloaded()
{
    return Impl_->AccessControlSnapshotsPreloaded();
}

bool TAccessControlManager::IsSuperuser(std::string_view subjectId) const
{
    return Impl_->IsSuperuser(subjectId);
}

TString TAccessControlManager::FormatPermissionValue(TAccessControlPermissionValue value) const
{
    return Impl_->FormatPermissionValue(value);
}

TAccessControlPermissionValue TAccessControlManager::CheckedPermissionValueCast(int value) const
{
    return Impl_->CheckedPermissionValueCast(value);
}

TPermissionCheckResult TAccessControlManager::CheckPermission(
    std::string_view subjectId,
    const NObjects::TObject* object,
    TAccessControlPermissionValue permission,
    const NYPath::TYPath& attributePath,
    std::source_location location)
{
    return Impl_->CheckPermission(
        subjectId,
        object,
        permission,
        attributePath,
        location);
}

std::vector<TPermissionCheckResult> TAccessControlManager::CheckPermissions(
    std::string_view subjectId,
    std::vector<TObjectPermission> permissions,
    std::source_location location)
{
    return Impl_->CheckPermissions(subjectId, std::move(permissions), location);
}

TPermissionCheckResult TAccessControlManager::CheckCachedPermission(
    std::string_view subjectId,
    NObjects::TObjectTypeValue objectType,
    const NObjects::TObjectKey& objectKey,
    TAccessControlPermissionValue permission,
    const NYPath::TYPath& attributePath)
{
    return Impl_->CheckCachedPermission(
        subjectId,
        objectType,
        objectKey,
        permission,
        attributePath);
}

TUserIdList TAccessControlManager::GetObjectAccessAllowedFor(
    const NObjects::TObject* object,
    TAccessControlPermissionValue permission,
    const NYPath::TYPath& attributePath,
    std::source_location location)
{
    return Impl_->GetObjectAccessAllowedFor(
        object,
        permission,
        attributePath,
        location);
}

TGetUserAccessAllowedToResult TAccessControlManager::GetUserAccessAllowedTo(
    std::string_view userId,
    TObjectTypeValue objectType,
    TAccessControlPermissionValue permission,
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

void TAccessControlManager::SetAuthenticatedUser(TAuthenticationIdentity identity, std::string userTicket)
{
    Impl_->SetAuthenticatedUser(std::move(identity), std::move(userTicket));
}

void TAccessControlManager::ResetAuthenticatedUser()
{
    Impl_->ResetAuthenticatedUser();
}

const TRequestTrackerPtr& TAccessControlManager::GetRequestTracker() const
{
    return Impl_->GetRequestTracker();
}

bool TAccessControlManager::ShouldPreloadAcl() const
{
    return Impl_->ShouldPreloadAcl();
}

void TAccessControlManager::ValidatePermissions(
    std::vector<TObjectPermission> requests,
    std::source_location location)
{
    Impl_->ValidatePermissions(std::move(requests), location);
}

void TAccessControlManager::ValidatePermission(
    const NObjects::TObject* object,
    TAccessControlPermissionValue permission,
    const NYPath::TYPath& attributePath,
    std::source_location location)
{
    Impl_->ValidatePermission(
        object,
        permission,
        attributePath,
        location);
}

void TAccessControlManager::ValidateSuperuser(TStringBuf doWhat)
{
    return Impl_->ValidateSuperuser(doWhat);
}

NYTree::IYPathServicePtr TAccessControlManager::CreateOrchidService()
{
    return Impl_->CreateOrchidService();
}

TAccessControlManagerConfigPtr TAccessControlManager::GetConfig() const
{
    return Impl_->GetConfig();
}

const TClusterSubjectSnapshotPtr TAccessControlManager::GetClusterSubjectSnapshot() const
{
    return Impl_->GetClusterSubjectSnapshot();
}

const TClusterSubjectSnapshotPtr TAccessControlManager::TryGetClusterSubjectSnapshot() const
{
    return Impl_->TryGetClusterSubjectSnapshot();
}

////////////////////////////////////////////////////////////////////////////////

TAuthenticatedUserGuard MakeAuthenticatedUserGuardAndThrottle(
    const IServiceContextPtr& context,
    const NAccessControl::TAccessControlManagerPtr &accessControlManager,
    i64 requestWeight,
    NClient::NObjects::TTransactionId transactionId)
{
    auto authenticatedUserGuard = MakeAuthenticatedUserGuard(context, accessControlManager);
    auto throttlerFuture = authenticatedUserGuard.ThrottleUserRequest(
        1,
        requestWeight,
        /*soft*/ !transactionId.IsEmpty());
    if (throttlerFuture != VoidFuture) {
        YT_LOG_DEBUG("Throttling request (RequestId: %v)", context->GetRequestId());
    }
    WaitFor(std::move(throttlerFuture))
        .ThrowOnError();
    return authenticatedUserGuard;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NAccessControl
