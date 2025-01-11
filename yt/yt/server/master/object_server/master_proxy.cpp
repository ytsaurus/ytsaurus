#include "features.h"
#include "master.h"
#include "private.h"
#include "type_handler_detail.h"
#include "object.h"

#include <yt/yt/server/master/cell_master/bootstrap.h>
#include <yt/yt/server/master/cell_master/config.h>
#include <yt/yt/server/master/cell_master/config_manager.h>

#include <yt/yt/server/master/chunk_server/chunk_manager.h>
#include <yt/yt/server/master/chunk_server/config.h>
#include <yt/yt/server/master/chunk_server/helpers.h>
#include <yt/yt/server/master/chunk_server/domestic_medium.h>

#include <yt/yt/server/master/cypress_server/cypress_manager.h>
#include <yt/yt/server/master/cypress_server/node_proxy_detail.h>

#include <yt/yt/server/master/maintenance_tracker_server/maintenance_request.h>
#include <yt/yt/server/master/maintenance_tracker_server/maintenance_tracker.h>

#include <yt/yt/server/master/node_tracker_server/node_tracker.h>
#include <yt/yt/server/master/node_tracker_server/node.h>
#include <yt/yt/server/master/node_tracker_server/node_directory_builder.h>
#include <yt/yt/server/master/node_tracker_server/node_discovery_manager.h>

#include <yt/yt/server/master/object_server/proto/object_manager.pb.h>

#include <yt/yt/server/master/security_server/security_manager.h>
#include <yt/yt/server/master/security_server/subject.h>
#include <yt/yt/server/master/security_server/acl.h>
#include <yt/yt/server/master/security_server/user.h>

#include <yt/yt/server/master/table_server/table_manager.h>

#include <yt/yt/ytlib/api/native/config.h>

#include <yt/yt/ytlib/cell_master_client/protobuf_helpers.h>

#include <yt/yt/ytlib/cypress_client/rpc_helpers.h>

#include <yt/yt/ytlib/election/config.h>

#include <yt/yt/ytlib/object_client/proto/master_ypath.pb.h>

#include <yt/yt/ytlib/tablet_client/helpers.h>

#include <yt/yt/core/misc/range_formatters.h>

#include <yt/yt/core/rpc/message.h>

#include <yt/yt/core/ytree/helpers.h>

#include <library/cpp/iterator/zip.h>

namespace NYT::NObjectServer {

using namespace NCellMaster;
using namespace NCellMasterClient;
using namespace NHydra;
using namespace NMaintenanceTrackerServer;
using namespace NNodeTrackerServer;
using namespace NObjectClient;
using namespace NObjectClient::NProto;
using namespace NRpc;
using namespace NSecurityServer;
using namespace NTableServer;
using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

static constexpr auto& Logger = ObjectServerLogger;

////////////////////////////////////////////////////////////////////////////////

class TMasterProxy
    : public TNonversionedObjectProxyBase<TMasterObject>
{
public:
    using TNonversionedObjectProxyBase::TNonversionedObjectProxyBase;

private:
    using TBase = TNonversionedObjectProxyBase<TMasterObject>;

    bool DoInvoke(const IYPathServiceContextPtr& context) override
    {
        DISPATCH_YPATH_SERVICE_METHOD(CreateObject);
        DISPATCH_YPATH_SERVICE_METHOD(GetClusterMeta,
            .SetHeavy(true)
            .SetResponseCodec(NCompression::ECodec::Lz4));
        DISPATCH_YPATH_SERVICE_METHOD(CheckPermissionByAcl);
        DISPATCH_YPATH_SERVICE_METHOD(AddMaintenance);
        DISPATCH_YPATH_SERVICE_METHOD(RemoveMaintenance);
        DISPATCH_YPATH_SERVICE_METHOD(MaterializeCopyPrerequisites);
        DISPATCH_YPATH_SERVICE_METHOD(MaterializeNode);
        DISPATCH_YPATH_SERVICE_METHOD(VectorizedRead);
        DISPATCH_YPATH_SERVICE_METHOD(GetOrRegisterTableSchema);
        return TBase::DoInvoke(context);
    }

    DECLARE_YPATH_SERVICE_METHOD(NObjectClient::NProto, CreateObject)
    {
        DeclareMutating();

        auto type = FromProto<EObjectType>(request->type());
        auto ignoreExisting = request->ignore_existing();

        context->SetRequestInfo("Type: %v, IgnoreExisting: %v",
            type,
            ignoreExisting);

        auto attributes = request->has_object_attributes()
            ? FromProto(request->object_attributes())
            : nullptr;

        const auto& objectManager = Bootstrap_->GetObjectManager();

        TObject* object = nullptr;
        if (ignoreExisting) {
            auto optionalExistingObject = objectManager->FindObjectByAttributes(type, attributes.Get());
            if (!optionalExistingObject) {
                THROW_ERROR_EXCEPTION("\"ignore_existing\" option is not supported for type %Qlv",
                    type);
            }

            if (*optionalExistingObject) {
                object = *optionalExistingObject;
                YT_LOG_DEBUG("Existing object returned (Id: %v)",
                    object->GetId());
            }
        }

        if (!object) {
            object = objectManager->CreateObject(
                /*hintId*/ NullObjectId,
                type,
                attributes.Get());

            YT_LOG_DEBUG("Object created (Id: %v, Type: %v)",
                object->GetId(),
                type);
        }

        ToProto(response->mutable_object_id(), object->GetId());

        const auto& handler = objectManager->GetHandler(object);
        if (Any(handler->GetFlags() & ETypeFlags::TwoPhaseCreation)) {
            response->set_two_phase_creation(true);
        }

        context->SetResponseInfo("ObjectId: %v", object->GetId());
        context->Reply();
    }

    DECLARE_YPATH_SERVICE_METHOD(NObjectClient::NProto, CheckPermissionByAcl)
    {
        DeclareNonMutating();

        const auto& securityManager = Bootstrap_->GetSecurityManager();

        auto* user = request->has_user()
            ? securityManager->GetUserByNameOrThrow(request->user(), true /*activeLifeStageOnly*/)
            : securityManager->GetAuthenticatedUser();
        auto permission = EPermission(request->permission());

        bool ignoreMissingSubjects = request->ignore_missing_subjects();

        context->SetRequestInfo("User: %v, Permission: %v",
            user->GetName(),
            permission);

        auto aclNode = ConvertToNode(TYsonString(request->acl()));
        TAccessControlList acl;
        if (ignoreMissingSubjects) {
            auto [deserializedAcl, missingSubjects] =
                DeserializeAclGatherMissingSubjectsOrThrow(aclNode, securityManager);
            ToProto(response->mutable_missing_subjects(), missingSubjects);
            acl = std::move(deserializedAcl);
        } else {
            acl = DeserializeAclOrThrow(aclNode, securityManager);
        }

        auto result = securityManager->CheckPermission(user, permission, acl);

        response->set_action(ToProto(result.Action));
        if (result.Subject) {
            ToProto(response->mutable_subject_id(), result.Subject->GetId());
            response->set_subject_name(ToProto(result.Subject->GetName()));
        }

        context->SetResponseInfo("Action: %v", result.Action);
        context->Reply();
    }

    DECLARE_YPATH_SERVICE_METHOD(NObjectClient::NProto, GetClusterMeta)
    {
        auto populateNodeDirectory = request->populate_node_directory();
        auto populateClusterDirectory = request->populate_cluster_directory();
        auto populateMediumDirectory = request->populate_medium_directory();
        auto populateCellDirectory = request->populate_cell_directory();
        auto populateMasterCacheNodeAddresses = request->populate_master_cache_node_addresses();
        auto populateTimestampProviderNodeAddresses = request->populate_timestamp_provider_node_addresses();
        auto populateFeatures = request->populate_features();
        auto populateUserDirectory = request->populate_user_directory();

        context->SetRequestInfo(
            "PopulateNodeDirectory: %v, "
            "PopulateClusterDirectory: %v, "
            "PopulateMediumDirectory: %v, "
            "PopulateCellDirectory: %v, "
            "PopulateMasterCacheNodeAddresses: %v, "
            "PopulateTimestampProviderNodeAddresses: %v, "
            "PopulateFeatures: %v, "
            "PopulateUserDirectory: %v",
            populateNodeDirectory,
            populateClusterDirectory,
            populateMediumDirectory,
            populateCellDirectory,
            populateMasterCacheNodeAddresses,
            populateTimestampProviderNodeAddresses,
            populateFeatures,
            populateUserDirectory);

        if (populateNodeDirectory) {
            TNodeDirectoryBuilder builder(response->mutable_node_directory());
            const auto& nodeTracker = Bootstrap_->GetNodeTracker();
            for (auto [nodeId, node] : nodeTracker->Nodes()) {
                if (!IsObjectAlive(node)) {
                    continue;
                }
                builder.Add(node);
            }
        }

        if (populateClusterDirectory) {
            const auto& objectManager = Bootstrap_->GetObjectManager();
            const auto& rootService = objectManager->GetRootService();
            auto mapNode = ConvertToNode(SyncYPathGet(rootService, NTabletClient::GetCypressClustersPath()))->AsMap();
            auto* protoClusterDirectory = response->mutable_cluster_directory();
            for (const auto& [key, child] : mapNode->GetChildren()) {
                auto* protoItem = protoClusterDirectory->add_items();
                protoItem->set_name(ToProto(key));
                protoItem->set_config(ConvertToYsonString(child).ToString());
            }
        }

        if (populateMediumDirectory) {
            SerializeMediumDirectory(response->mutable_medium_directory(), Bootstrap_->GetChunkManager());
        }

        if (populateCellDirectory) {
            const auto& multicellManager = Bootstrap_->GetMulticellManager();
            const auto& masterCellConnectionConfigs = multicellManager->GetMasterCellConnectionConfigs();
            auto* protoCellDirectory = response->mutable_cell_directory();

            auto addCell = [&] (const auto& cellConfig) {
                auto* cellDirectoryItem = protoCellDirectory->add_items();
                ToProto(cellDirectoryItem, cellConfig);

                auto roles = multicellManager->GetMasterCellRoles(CellTagFromId(cellConfig->CellId));
                for (auto role : TEnumTraits<EMasterCellRole>::GetDomainValues()) {
                    if (Any(roles & EMasterCellRoles(role))) {
                        cellDirectoryItem->add_roles(static_cast<i32>(role));
                    }
                }
            };

            addCell(masterCellConnectionConfigs->PrimaryMaster);
            for (const auto& secondaryMasterConfig : masterCellConnectionConfigs->SecondaryMasters) {
                addCell(secondaryMasterConfig);
            }
        }

        if (populateMasterCacheNodeAddresses) {
            const auto& masterCacheNodeAddresses = Bootstrap_->GetNodeTracker()->GetNodeAddressesForRole(
                NNodeTrackerClient::ENodeRole::MasterCache);
            ToProto(response->mutable_master_cache_node_addresses(), masterCacheNodeAddresses);
        }

        if (populateTimestampProviderNodeAddresses) {
            const auto& timestampProviderAddresses = Bootstrap_->GetNodeTracker()->GetNodeAddressesForRole(
                NNodeTrackerClient::ENodeRole::TimestampProvider);
            ToProto(response->mutable_timestamp_provider_node_addresses(), timestampProviderAddresses);
        }

        if (populateFeatures) {
            const auto& configManager = Bootstrap_->GetConfigManager();
            const auto& chunkManagerConfig = configManager->GetConfig()->ChunkManager;
            response->set_features(CreateFeatureRegistryYson(chunkManagerConfig->ForbiddenCompressionCodecs).ToString());
        }

        if (populateUserDirectory) {
            const auto& securityManager = Bootstrap_->GetSecurityManager();
            auto* protoUserDirectory = response->mutable_user_directory();

            auto addUser = [&] (TUser* user) {
                auto* protoLimits = protoUserDirectory->add_limits();
                protoLimits->set_user_name(ToProto(user->GetName()));

                auto limits = user->GetObjectServiceRequestLimits();
                if (auto limit = limits->ReadRequestRateLimits->Default) {
                    protoLimits->set_read_request_rate_limit(*limit);
                }
                if (auto limit = limits->WriteRequestRateLimits->Default) {
                    protoLimits->set_write_request_rate_limit(*limit);
                }
                protoLimits->set_request_queue_size_limit(limits->RequestQueueSizeLimits->Default);
            };

            for (auto [userId, user] : securityManager->Users()) {
                addUser(user);
            }
        }

        context->Reply();
    }

    DECLARE_YPATH_SERVICE_METHOD(NObjectClient::NProto, AddMaintenance)
    {
        DeclareMutating();

        auto component = FromProto<EMaintenanceComponent>(request->component());
        auto type = FromProto<EMaintenanceType>(request->type());

        context->SetRequestInfo(
            "Component: %v, "
            "Address: %v, "
            "Type: %v, "
            "Comment: %v, "
            "SupportsPerTargetResponse: %v",
            component,
            request->address(),
            type,
            request->comment(),
            request->supports_per_target_response());

        if (component == EMaintenanceComponent::ClusterNode || component == EMaintenanceComponent::Host) {
            const auto& multicellManager = Bootstrap_->GetMulticellManager();
            if (!multicellManager->IsPrimaryMaster()) {
                THROW_ERROR_EXCEPTION("Cluster node maintenance can only be added via primary master");
            }
        }

        std::optional<NCypressServer::TNodeId> componentRegistry;
        if (request->has_component_registry_id()) {
            FromProto(&componentRegistry.emplace(), request->component_registry_id());
        }

        const auto& maintenanceTracker = Bootstrap_->GetMaintenanceTracker();
        auto ids = maintenanceTracker->AddMaintenance(
            component,
            request->address(),
            type,
            request->comment(),
            componentRegistry);

        // COMPAT(kvk1920): Compatibility with pre-24.2 native clients.
        if (!request->supports_per_target_response()) {
            ToProto(
                response->mutable_id(),
                ids.size() == 1 ? ids.begin()->second : TMaintenanceId{});
        } else {
            // Result have to be sorted here because of Persistent Response
            // Keeper. TCompactFlatMap is sorted so just check that nobody
            // changed the TMaintenanceIdPerTarget type alias.
            static_assert(std::is_same_v<TMaintenanceIdPerTarget, TCompactFlatMap<std::string, TMaintenanceId, 1>>);
            YT_ASSERT(std::is_sorted(ids.begin(), ids.end()));

            for (const auto& [target, id] : ids) {
                auto* proto = response->add_ids();
                proto->set_target(ToProto(target));
                ToProto(proto->mutable_id(), id);
            }
        }
        context->Reply();
    }

    DECLARE_YPATH_SERVICE_METHOD(NObjectClient::NProto, RemoveMaintenance)
    {
        DeclareMutating();

        auto component = FromProto<EMaintenanceComponent>(request->component());

        std::optional<TCompactSet<TMaintenanceId, TypicalMaintenanceRequestCount>> ids;
        if (!request->ids().empty()) {
            ids.emplace();
            for (auto id : request->ids()) {
                ids->insert(FromProto<TMaintenanceId>(id));
            }
        }

        std::optional<std::string> user;
        if (request->has_user()) {
            user = request->user();
        } else if (request->mine()) {
            const auto& securityManager = Bootstrap_->GetSecurityManager();
            user = securityManager->GetAuthenticatedUser()->GetName();
        }

        std::optional<EMaintenanceType> type;
        if (request->has_type()) {
            type = FromProto<EMaintenanceType>(request->type());
        }

        context->SetRequestInfo(
            "Component: %v, Address: %v, Ids: %v, User: %v, Type: %v, SupportsPerTargetResponse: %v",
            component,
            request->address(),
            ids ? std::optional(TCompactVector<TMaintenanceId, TypicalMaintenanceRequestCount>(ids->begin(), ids->end())) : std::nullopt,
            user,
            type,
            request->supports_per_target_response());

        if (component == EMaintenanceComponent::ClusterNode || component == EMaintenanceComponent::Host) {
            const auto& multicellManager = Bootstrap_->GetMulticellManager();
            if (!multicellManager->IsPrimaryMaster()) {
                THROW_ERROR_EXCEPTION("Cluster node maintenance can only be added via primary master");
            }
        }

        THROW_ERROR_EXCEPTION_IF(request->has_user() && request->mine(),
            "At most one of {\"mine\", \"user\"} can be specified");

        const auto& maintenanceTracker = Bootstrap_->GetMaintenanceTracker();
        std::optional<NCypressServer::TNodeId> componentRegistry;
        if (request->has_component_registry_id()) {
            FromProto(&componentRegistry.emplace(), request->component_registry_id());
        }
        auto removedMaintenanceCounts = maintenanceTracker->RemoveMaintenance(
            component,
            request->address(),
            ids,
            user,
            type,
            componentRegistry);

        auto fillMaintenanceCount = [] (
            auto* proto,
            EMaintenanceType type,
            const TMaintenanceCounts& counts) {
                proto->set_type(ToProto(type));
                proto->set_count(counts[type]);
            };

        // COMPAT(kvk1920): Compatibility with pre-24.2 proxies.
        if (!request->supports_per_target_response()) {
            TMaintenanceCounts totalCounts;
            for (const auto& [target, counts] : removedMaintenanceCounts) {
                for (auto type : TEnumTraits<EMaintenanceType>::GetDomainValues()) {
                    totalCounts[type] += counts[type];
                }
            }

            for (auto type : TEnumTraits<EMaintenanceType>::GetDomainValues()) {
                fillMaintenanceCount(response->add_removed_maintenance_counts(), type, totalCounts);
            }
        } else {
            response->set_supports_per_target_response(true);
            for (const auto& [target, counts] : removedMaintenanceCounts) {
                auto* perTargetResponse = response->add_removed_maintenance_counts_per_target();
                perTargetResponse->set_target(ToProto(target));
                for (auto type : TEnumTraits<EMaintenanceType>::GetDomainValues()) {
                    fillMaintenanceCount(perTargetResponse->add_counts(), type, counts);
                }
            }
        }

        context->Reply();
    }

    DECLARE_YPATH_SERVICE_METHOD(NObjectClient::NProto, MaterializeCopyPrerequisites)
    {
        DeclareMutating();

        auto transactionId = NCypressClient::GetTransactionId(context);

        // Using (essentially) two for loops here because schema can be quite heavy,
        // and storing them in a vector is costly.
        context->SetRequestInfo("TransactionId: %v, SchemaCount: %v, OldSchemaIds: %v",
            transactionId,
            request->schema_id_to_schema_mapping_size(),
            std::views::transform(
                request->schema_id_to_schema_mapping(),
                [] (const auto& entry) {
                    return FromProto<TMasterTableSchemaId>(entry.schema_id());
                }));

        // Doing this just to offload schema destruction to other thread.
        std::vector<NTableClient::TTableSchema> processedSchemas;
        processedSchemas.reserve(request->schema_id_to_schema_mapping_size());
        auto offloadSchemaDestruction = Finally([&] {
            NRpc::TDispatcher::Get()->GetHeavyInvoker()
                ->Invoke(BIND([processedSchemas = std::move(processedSchemas)] { }));
        });

        const auto& transactionManager = Bootstrap_->GetTransactionManager();
        auto* transaction = transactionManager->GetTransactionOrThrow(transactionId);

        response->mutable_updated_schema_id_mapping()->Reserve(request->schema_id_to_schema_mapping_size());
        const auto& tableManager = Bootstrap_->GetTableManager();
        for (auto entry : request->schema_id_to_schema_mapping()) {
            auto oldSchemaId = FromProto<TMasterTableSchemaId>(entry.schema_id());
            // Out of love for paranoia.
            YT_VERIFY(oldSchemaId);

            auto schema = FromProto<NTableClient::TTableSchema>(entry.schema());

            // NB: Schema lifetime is managed by cross-cell copy transaction.
            auto masterTableSchema = tableManager->GetOrCreateNativeMasterTableSchema(schema, transaction);

            auto* rspEntry = response->add_updated_schema_id_mapping();
            ToProto(rspEntry->mutable_old_schema_id(), oldSchemaId);
            ToProto(rspEntry->mutable_new_schema_id(), masterTableSchema->GetId());

            processedSchemas.push_back(std::move(schema));
        }

        context->SetResponseInfo("SchemaIdMapping: %v",
            MakeShrunkFormattableView(
                response->updated_schema_id_mapping(),
                [] (
                    TStringBuilderBase* builder,
                    const auto& entry
                ) {
                    auto oldId = FromProto<TMasterTableSchemaId>(entry.old_schema_id());
                    auto newId = FromProto<TMasterTableSchemaId>(entry.new_schema_id());
                    builder->AppendFormat("%v -> %v", oldId, newId);
                },
                /*limit*/ 100));

        context->Reply();
    }

    DECLARE_YPATH_SERVICE_METHOD(NObjectClient::NProto, MaterializeNode)
    {
        DeclareMutating();

        auto transactionId = NCypressClient::GetTransactionId(context);
        const auto& transactionManager = Bootstrap_->GetTransactionManager();
        auto* transaction = transactionManager->GetTransactionOrThrow(transactionId);

        const auto& serializedNode = request->serialized_node();
        auto mode = FromProto<NCypressClient::ENodeCloneMode>(request->mode());
        auto newAccountId = request->has_new_account_id()
            ? std::make_optional(FromProto<TAccountId>(request->new_account_id()))
            : std::nullopt;
        bool preserveCreationTime = request->preserve_creation_time();
        bool preserveExpirationTime = request->preserve_expiration_time();
        bool preserveExpirationTimeout = request->preserve_expiration_timeout();
        bool preserveOwner = request->preserve_owner();
        auto pessimisticQuotaCheck = request->pessimistic_quota_check();
        auto existingNodeId = FromProto<NCypressServer::TNodeId>(request->existing_node_id());

        context->SetRequestInfo(
            "DataSize: %v, Mode: %v, TransactionId: %v, PreserveAccount: %v, PreserveCreationTime: %v, PreserveExpirationTime: %v, "
            "PreserveExpirationTimeout: %v, PreserveOwner: %v, PessimisticQuotaCheck: %v, ExistingNodeId: %v",
            serializedNode.data().size(),
            mode,
            transactionId,
            !newAccountId.has_value(),
            preserveCreationTime,
            preserveExpirationTime,
            preserveExpirationTimeout,
            preserveOwner,
            pessimisticQuotaCheck,
            existingNodeId);

        auto version = request->version();
        if (version != NCellMaster::GetCurrentReign()) {
            THROW_ERROR_EXCEPTION("Invalid node metadata format version: expected %v, actual %v",
                NCellMaster::GetCurrentReign(),
                version);
        }

        // Presence of a new account means that user does not want to preserve an old one.
        TAccount* account = nullptr;
        const auto& securityManager = Bootstrap_->GetSecurityManager();
        if (newAccountId) {
            account = securityManager->GetAccountOrThrow(*newAccountId);
            const auto& objectManager = Bootstrap_->GetObjectManager();
            objectManager->ValidateObjectLifeStage(account);
        }

        const auto& cypressManager = Bootstrap_->GetCypressManager();
        auto factory = cypressManager->CreateNodeFactory(
            /*shard*/ nullptr, // Shard will be set when assembling the tree.
            transaction,
            account,
            NCypressServer::TNodeFactoryOptions{
                .PreserveAccount = !newAccountId.has_value(),
                .PreserveCreationTime = preserveCreationTime,
                .PreserveModificationTime = true, // Modification time will be changed when assembling subtree, if needed.
                .PreserveExpirationTime = preserveExpirationTime,
                .PreserveExpirationTimeout = preserveExpirationTimeout,
                .PreserveOwner = preserveOwner,
                .PreserveAcl = true, // Same as modification time.
                .PessimisticQuotaCheck = pessimisticQuotaCheck
            },
            /*serviceTrunkNode*/ nullptr,
            /*unresolvedSuffix*/ {}); // Unused during copy, relevant only for "create".

        NCypressServer::TMaterializeNodeContext copyContext(
            Bootstrap_,
            mode,
            TRef::FromString(serializedNode.data()),
            FromProto<TMasterTableSchemaId>(serializedNode.schema_id()),
            existingNodeId);

        auto inheritedAttributes = request->has_inherited_attributes_override()
            ? FromProto(request->inherited_attributes_override())
            : New<NCypressServer::TInheritedAttributeDictionary>(Bootstrap_);

        auto* node = factory->MaterializeNode(inheritedAttributes.Get(), &copyContext);

        // TODO(h0pless): When expanding list of inherited attributes re-calculated during copy, some trivial
        // setter code should be written somewhere here.
        // Since only chunk_merger_mode is supported right now it's fine to leave it as-is.
        factory->Commit();

        response->mutable_old_node_id()->CopyFrom(serializedNode.node_id());
        ToProto(response->mutable_new_node_id(), node->GetId());

        auto oldId = FromProto<NCypressServer::TNodeId>(serializedNode.node_id());
        context->SetResponseInfo("OldId: %v, NewId: %v, TransactionId: %v, Account: %v",
            oldId,
            node->GetId(),
            transactionId,
            node->Account()->GetName());

        context->Reply();
    }

    DECLARE_YPATH_SERVICE_METHOD(NObjectClient::NProto, VectorizedRead)
    {
        DeclareNonMutating();

        auto objectIds = FromProto<std::vector<TObjectId>>(request->object_ids());

        // Recover template request.
        const auto& requestAttachments = context->RequestAttachments();
        auto templateRequestPartCount = request->template_request_part_count();
        TSharedRefArrayBuilder templateRequestBuilder(templateRequestPartCount);
        for (int partIndex = 0; partIndex < templateRequestPartCount; ++partIndex) {
            templateRequestBuilder.Add(requestAttachments[partIndex]);
        }
        auto templateRequest = templateRequestBuilder.Finish();

        NRpc::NProto::TRequestHeader templateRequestHeader;
        if (!TryParseRequestHeader(templateRequest, &templateRequestHeader)) {
            THROW_ERROR_EXCEPTION("Error parsing request header");
        }
        auto templateMethod = templateRequestHeader.method();
        auto transactionId = NCypressClient::GetTransactionId(context->RequestHeader());

        context->SetRequestInfo("TemplateMethod: %v, TransactionId: %v, ObjectIds: %v",
            templateMethod,
            transactionId,
            objectIds);

        ValidateVectorizedRead(templateMethod, objectIds);
        // NB: No need to sync with TX coordinator here, since this request is designed to be used in conjunction with batch request
        // and the latter handles all needed syncs.
        const auto& transactionManager = Bootstrap_->GetTransactionManager();
        auto* transaction = transactionId
            ? transactionManager->GetTransactionOrThrow(transactionId)
            : nullptr;

        auto onSubresponseCallback = [context, response] (TObjectId objectId, const TSharedRefArray& message) {
            auto& attachments = context->Response().Attachments();
            attachments.insert(attachments.end(), message.Begin(), message.End());
            auto* subresponse = response->add_subresponses();
            subresponse->set_part_count(message.size());
            ToProto(subresponse->mutable_object_id(), objectId);
        };

        std::vector<TFuture<TSharedRefArray>> futures;
        std::vector<TObjectId> objectIdsWithAsyncRequests;
        const auto& objectManager = Bootstrap_->GetObjectManager();
        for (auto objectId : objectIds) {
            // It's impossible to clear response without wiping the whole context, so it has to be created anew for each node.
            auto subcontext = CreateYPathContext(templateRequest, Logger());

            if (auto* object = objectManager->FindObject(objectId)) {
                auto proxy = objectManager->GetProxy(object, transaction);
                proxy->Invoke(subcontext);
            } else {
                subcontext->Reply(TError(
                    NYTree::EErrorCode::ResolveError,
                    "No such object %v",
                    objectId));
            }

            if (subcontext->IsReplied()) {
                onSubresponseCallback(objectId, subcontext->GetResponseMessage());
            } else {
                // To avoid unnecessary conflicts, we populate the attachments only after
                // all asynchronous requests are executed.
                futures.push_back(subcontext->GetAsyncResponseMessage());
                objectIdsWithAsyncRequests.push_back(objectId);
            }
        }

        // Shortcut for synchronous response.
        if (futures.empty()) {
            context->Reply();
        } else {
            AllSet(std::move(futures))
                .Subscribe(BIND([
                    context,
                    onSubresponseCallback,
                    objectIds = std::move(objectIdsWithAsyncRequests)
                ] (TErrorOr<std::vector<TErrorOr<TSharedRefArray>>> allSetResult) {
                    if (!allSetResult.IsOK()) {
                        context->Reply(allSetResult);
                        return;
                    }

                    const auto& results = allSetResult.Value();
                    for (const auto& [objectId, result] : Zip(objectIds, results)) {
                        if (result.IsOK()) {
                            onSubresponseCallback(objectId, result.Value());
                        } else {
                            auto message = CreateErrorResponseMessage(result);
                            onSubresponseCallback(objectId, message);
                        }
                    }

                    context->Reply();
                }));
        }
    }

    void ValidateVectorizedRead(const std::string& templateMethod, const std::vector<TObjectId>& objectIds)
    {
        static const int MaxVectorizedReadRequestSize = 100;
        static const THashSet<std::string> VectorizedReadMethodWhitelist = {
            "Get",
            "SerializeNode",
        };

        THROW_ERROR_EXCEPTION_UNLESS(
            VectorizedReadMethodWhitelist.contains(templateMethod),
            "Method %Qv is not supported as a template for \"VectorizedRead\"",
            templateMethod);

        if (objectIds.size() > MaxVectorizedReadRequestSize) {
            THROW_ERROR_EXCEPTION("Too many objects in vectorized request: %v > %v",
                objectIds.size(),
                MaxVectorizedReadRequestSize);
        }
    }

    DECLARE_YPATH_SERVICE_METHOD(NObjectClient::NProto, GetOrRegisterTableSchema)
    {
        DeclareMutating();
        context->SetRequestInfo(
            "Schema: %v, TransactionId: %v",
            request->schema(),
            request->transaction_id());

        auto schema = FromProto<NTableClient::TTableSchemaPtr>(request->schema());

        auto transactionId = FromProto<TTransactionId>(request->transaction_id());
        const auto& transactionManager = Bootstrap_->GetTransactionManager();
        auto* transaction = transactionManager->GetTransactionOrThrow(transactionId);

        const auto& tableManager = Bootstrap_->GetTableManager();
        auto result = tableManager->GetOrCreateNativeMasterTableSchema(*schema, transaction);
        ToProto(response->mutable_schema_id(), result->GetId());

        context->SetResponseInfo(
            "SchemaId: %v",
            result->GetId());

        context->Reply();
    }
};

////////////////////////////////////////////////////////////////////////////////

IObjectProxyPtr CreateMasterProxy(
    TBootstrap* bootstrap,
    TObjectTypeMetadata* metadata,
    TMasterObject* object)
{
    return New<TMasterProxy>(bootstrap, metadata, object);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NObjectServer
