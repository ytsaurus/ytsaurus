#include "portal_manager.h"

#include "cypress_manager.h"
#include "helpers.h"
#include "portal_entrance_node.h"
#include "portal_exit_node.h"
#include "private.h"
#include "public.h"
#include "shard.h"

#include <yt/yt/server/master/cell_master/bootstrap.h>
#include <yt/yt/server/master/cell_master/config.h>
#include <yt/yt/server/master/cell_master/config_manager.h>
#include <yt/yt/server/master/cell_master/hydra_facade.h>
#include <yt/yt/server/master/cell_master/multicell_manager.h>
#include <yt/yt/server/master/cell_master/serialize.h>

#include <yt/yt/server/master/cypress_server/proto/portal_manager.pb.h>

#include <yt/yt/server/master/object_server/object_manager.h>

#include <yt/yt/server/master/security_server/acl.h>
#include <yt/yt/server/master/security_server/helpers.h>
#include <yt/yt/server/master/security_server/security_manager.h>

#include <yt/yt/server/lib/misc/interned_attributes.h>

#include <yt/yt/ytlib/api/native/proto/transaction_actions.pb.h>

#include <yt/yt/core/concurrency/periodic_executor.h>
#include <yt/yt/core/concurrency/thread_affinity.h>

#include <yt/yt/core/ytree/helpers.h>

namespace NYT::NCypressServer {

using namespace NCellMaster;
using namespace NHiveServer;
using namespace NHydra;
using namespace NObjectClient;
using namespace NObjectServer;
using namespace NSecurityServer;
using namespace NTransactionServer;
using namespace NTransactionSupervisor;
using namespace NYson;
using namespace NYTree;

using namespace NApi::NNative::NProto;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = CypressServerLogger;

////////////////////////////////////////////////////////////////////////////////

class TPortalManager
    : public IPortalManager
    , public TMasterAutomatonPart
{
public:
    explicit TPortalManager(NCellMaster::TBootstrap* bootstrap)
        : TMasterAutomatonPart(bootstrap, NCellMaster::EAutomatonThreadQueue::PortalManager)
    {
        RegisterLoader(
            "PortalManager.Keys",
            BIND(&TPortalManager::LoadKeys, Unretained(this)));
        RegisterLoader(
            "PortalManager.Values",
            BIND(&TPortalManager::LoadValues, Unretained(this)));

        RegisterSaver(
            ESyncSerializationPriority::Keys,
            "PortalManager.Keys",
            BIND(&TPortalManager::SaveKeys, Unretained(this)));
        RegisterSaver(
            ESyncSerializationPriority::Values,
            "PortalManager.Values",
            BIND(&TPortalManager::SaveValues, Unretained(this)));

        RegisterMethod(BIND_NO_PROPAGATE(&TPortalManager::HydraCreatePortalExit, Unretained(this)));
        RegisterMethod(BIND_NO_PROPAGATE(&TPortalManager::HydraRemovePortalEntrance, Unretained(this)));
        RegisterMethod(BIND_NO_PROPAGATE(&TPortalManager::HydraRemovePortalExit, Unretained(this)));
        RegisterMethod(BIND_NO_PROPAGATE(&TPortalManager::HydraSynchronizePortalExit, Unretained(this)));

        SynchronizePortalExitsExecutor_ = New<NConcurrency::TPeriodicExecutor>(
            Bootstrap_->GetHydraFacade()->GetAutomatonInvoker(NCellMaster::EAutomatonThreadQueue::PortalManager),
            BIND(&TPortalManager::OnSynchronizePortalExits, MakeWeak(this)));
        SynchronizePortalExitsExecutor_->Start();

        const auto& configManager = Bootstrap_->GetConfigManager();
        configManager->SubscribeConfigChanged(BIND_NO_PROPAGATE(&TPortalManager::OnDynamicConfigChanged, MakeWeak(this)));
    }

    void Initialize() override
    {
        const auto& transactionManager = Bootstrap_->GetTransactionManager();

        transactionManager->RegisterTransactionActionHandlers<TReqCopySynchronizablePortalAttributes>({
            .Prepare = BIND_NO_PROPAGATE(&TPortalManager::HydraCopySynchronizablePortalAttributes, Unretained(this)),
        });
    }

    void OnDynamicConfigChanged(TDynamicClusterConfigPtr /*oldConfig*/)
    {
        const auto& configManager = Bootstrap_->GetConfigManager();
        const auto& config = configManager->GetConfig()->CypressManager;
        SynchronizePortalExitsExecutor_->SetPeriod(config->PortalSynchronizationPeriod);
    }

    void OnSynchronizePortalExits()
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        if (!IsLeader()) {
            return;
        }

        const auto& configManager = Bootstrap_->GetConfigManager();
        const auto& config = configManager->GetConfig()->CypressManager;
        if (!config->EnablePortalSynchronization) {
            return;
        }

        const auto& multicellManager = Bootstrap_->GetMulticellManager();
        const auto& securityManager = Bootstrap_->GetSecurityManager();

        THashMap<TCellTag, std::vector<TPortalEntranceNode*>> portalsByCellTag;

        for (auto [nodeId, node] : EntranceNodes_) {
            portalsByCellTag[node->GetExitCellTag()].push_back(node);
        }

        for (auto [exitCellTag, portals] : portalsByCellTag) {
            NProto::TReqSynchronizePortalExits request;

            for (auto* node : portals) {
                YT_VERIFY(node->IsTrunk());
                YT_VERIFY(node->GetExitCellTag() == exitCellTag);

                const auto& cypressManager = Bootstrap_->GetCypressManager();

                auto* portalExitInfo = request.add_portal_infos();
                ToProto(portalExitInfo->mutable_node_id(), MakePortalExitNodeId(node->GetId(), node->GetExitCellTag()));

                // NB: The following things are synchronizable:
                // 1) effective inheritable attributes
                // 2) effective acl
                // 3) effective annotation
                // 4) annotation path
                // 5) owner
                // 6) inherit_acl
                // 7) direct acl

                auto effectiveInheritableAttributes = New<TInheritedAttributeDictionary>(Bootstrap_);
                GatherInheritableAttributes(node->GetParent(), &effectiveInheritableAttributes->Attributes());
                ToProto(portalExitInfo->mutable_effective_inheritable_attributes(), *effectiveInheritableAttributes);

                auto effectiveAcl = securityManager->GetEffectiveAcl(node);
                auto serializedEffectiveAcl = ConvertToYsonString(effectiveAcl).ToString();
                portalExitInfo->set_effective_acl(serializedEffectiveAcl);

                portalExitInfo->set_direct_acl(ConvertToYsonString(node->Acd().Acl()).ToString());

                portalExitInfo->set_inherit_acl(node->Acd().Inherit());

                portalExitInfo->set_owner(node->Acd().GetOwner()->GetName());

                if (auto annotationNode = FindClosestAncestorWithAnnotation(node)) {
                    portalExitInfo->mutable_effective_annotation()->set_annotation(*annotationNode->TryGetAnnotation());
                    portalExitInfo->mutable_effective_annotation()->set_annotation_path(
                        cypressManager->GetNodePath(annotationNode, /*transaction*/ nullptr));
                }
            }

            // NB: We do not actually need to know if this message was delivered,
            // so we send this message with reliable = false in order to avoid mutation creating.
            multicellManager->PostToMaster(request, exitCellTag, /*reliable*/ false);
        }
    }

    void RegisterEntranceNode(
        TPortalEntranceNode* node,
        const IAttributeDictionary& inheritedAttributes,
        const IAttributeDictionary& explicitAttributes) override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        auto* trunkNode = node->GetTrunkNode()->As<TPortalEntranceNode>();
        auto* transaction = node->GetTransaction();

        const auto& cypressManager = Bootstrap_->GetCypressManager();
        auto path = cypressManager->GetNodePath(trunkNode, transaction);

        const auto& securityManager = Bootstrap_->GetSecurityManager();
        auto effectiveAcl = securityManager->GetEffectiveAcl(trunkNode);
        auto effectiveAnnotation = GetEffectiveAnnotation(node);

        NProto::TReqCreatePortalExit request;
        ToProto(request.mutable_entrance_node_id(), trunkNode->GetId());
        ToProto(request.mutable_account_id(), trunkNode->Account()->GetId());
        request.set_path(path);
        request.set_effective_acl(ConvertToYsonString(effectiveAcl).ToString());
        request.set_direct_acl(ConvertToYsonString(trunkNode->Acd().Acl()).ToString());
        request.set_inherit_acl(trunkNode->Acd().Inherit());
        ToProto(request.mutable_inherited_node_attributes(), inheritedAttributes);
        ToProto(request.mutable_explicit_node_attributes(), explicitAttributes);
        ToProto(request.mutable_parent_id(), trunkNode->GetParent()->GetId());
        if (auto optionalKey = FindNodeKey(cypressManager, trunkNode, transaction)) {
            request.set_key(*optionalKey);
        }

        if (effectiveAnnotation) {
            auto* annotationNode = FindClosestAncestorWithAnnotation(node);
            YT_VERIFY(annotationNode);

            request.set_effective_annotation(*effectiveAnnotation);
            request.set_effective_annotation_path(cypressManager->GetNodePath(annotationNode, transaction));
        }

        const auto& multicellManager = Bootstrap_->GetMulticellManager();
        multicellManager->PostToMaster(request, trunkNode->GetExitCellTag());

        YT_VERIFY(EntranceNodes_.emplace(trunkNode->GetId(), trunkNode).second);

        YT_LOG_DEBUG(
            "Portal entrance registered (EntranceNodeId: %v, ExitCellTag: %v, Account: %v, Path: %v)",
            trunkNode->GetId(),
            trunkNode->GetExitCellTag(),
            trunkNode->Account()->GetName(),
            path);
    }

    void DestroyEntranceNode(TPortalEntranceNode* trunkNode) override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(trunkNode->IsTrunk());

        if (EntranceNodes_.erase(trunkNode->GetId()) != 1) {
            return;
        }

        NProto::TReqRemovePortalExit request;
        ToProto(request.mutable_node_id(), MakePortalExitNodeId(trunkNode->GetId(), trunkNode->GetExitCellTag()));
        const auto& multicellManager = Bootstrap_->GetMulticellManager();
        multicellManager->PostToMaster(request, trunkNode->GetExitCellTag());

        YT_LOG_DEBUG("Portal entrance unregistered (NodeId: %v)",
            trunkNode->GetId());
    }

    void DestroyExitNode(TPortalExitNode* trunkNode) override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(trunkNode->IsTrunk());

        if (ExitNodes_.erase(trunkNode->GetId()) != 1) {
            return;
        }

        NProto::TReqRemovePortalEntrance request;
        ToProto(request.mutable_entrance_node_id(), MakePortalEntranceNodeId(trunkNode->GetId(), trunkNode->GetEntranceCellTag()));
        const auto& multicellManager = Bootstrap_->GetMulticellManager();
        multicellManager->PostToMaster(request, trunkNode->GetEntranceCellTag());

        YT_LOG_DEBUG("Portal exit unregistered (NodeId: %v)",
            trunkNode->GetId());
    }

    const TEntranceNodeMap& GetEntranceNodes() override
    {
        return EntranceNodes_;
    }

    const TExitNodeMap& GetExitNodes() override
    {
        return ExitNodes_;
    }

private:
    DECLARE_THREAD_AFFINITY_SLOT(AutomatonThread);

    NConcurrency::TPeriodicExecutorPtr SynchronizePortalExitsExecutor_;

    TEntranceNodeMap EntranceNodes_;
    TExitNodeMap ExitNodes_;

    void SaveKeys(NCellMaster::TSaveContext& /*context*/) const
    { }

    void SaveValues(NCellMaster::TSaveContext& context) const
    {
        using NYT::Save;
        Save(context, EntranceNodes_);
        Save(context, ExitNodes_);
    }


    void LoadKeys(NCellMaster::TLoadContext& /*context*/)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
    }

    void LoadValues(NCellMaster::TLoadContext& context)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        using NYT::Load;
        Load(context, EntranceNodes_);
        Load(context, ExitNodes_);
    }

    void Clear() override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        TMasterAutomatonPart::Clear();

        EntranceNodes_.clear();
        ExitNodes_.clear();
    }

    void OnBeforeSnapshotLoaded() override
    {
        TMasterAutomatonPart::OnBeforeSnapshotLoaded();
    }

    TSubject* DeserializeOwner(const TString& ownerName)
    {
        const auto& securityManager = Bootstrap_->GetSecurityManager();

        auto* owner = securityManager->FindSubjectByNameOrAlias(ownerName, /*activeLifeStageOnly*/ false);
        if (!owner) {
            YT_LOG_ALERT("Serialized subject is missing (SubjectNameOrAlias: %v)",
                ownerName);
        }

        return owner;
    }

    void HydraCopySynchronizablePortalAttributes(
        TTransaction* transaction,
        TReqCopySynchronizablePortalAttributes* request,
        const TTransactionPrepareOptions& prepareOptions)
    {
        YT_VERIFY(prepareOptions.Persistent);

        auto sourceNodeId = FromProto<TNodeId>(request->source_node_id());
        auto destinationNodeId = FromProto<TNodeId>(request->destination_node_id());

        const auto& cypressManager = Bootstrap_->GetCypressManager();
        auto* sourceNode = cypressManager->FindNode(TVersionedNodeId(sourceNodeId));
        auto* destinationNode = cypressManager->FindNode(TVersionedNodeId(destinationNodeId));
        if (!sourceNode || !destinationNode) {
            YT_LOG_INFO("Failed to copy synchronizable portal attributes; %v node does not exist"
                "(TransactionId: %v, SourceNodeId: %v, DestinationNodeId: %v)",
                sourceNode ? "destination" : "source",
                transaction->GetId(),
                sourceNodeId,
                destinationNodeId);
            return;
        }

        const auto& securityManager = Bootstrap_->GetSecurityManager();
        auto* sourceAcd = securityManager->FindAcd(sourceNode);
        auto* destinationAcd = securityManager->FindAcd(destinationNode);

        destinationAcd->SetEntries(sourceAcd->Acl());
        destinationAcd->SetOwner(sourceAcd->GetOwner());
        destinationAcd->SetInherit(sourceAcd->Inherit());
        if (auto annotation = sourceNode->TryGetAnnotation()) {
            destinationNode->SetAnnotation(std::move(*annotation));
        } else {
            destinationNode->RemoveAnnotation();
        }
    }

    void HydraSynchronizePortalExit(NProto::TReqSynchronizePortalExits* request)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        int synchronizedPortalsCount = 0;

        for (const auto& portalExitInfo : request->portal_infos()) {
            auto nodeId = FromProto<TObjectId>(portalExitInfo.node_id());
            auto it = ExitNodes_.find(nodeId);
            if (it == ExitNodes_.end()) {
                YT_LOG_ERROR("Skipping unknown portal exit synchronization (NodeId: %v)",
                    nodeId);
                continue;
            }

            TPortalExitNode* exitNode = it->second;

            try {
                auto inheritableAttributes = New<TInheritedAttributeDictionary>(Bootstrap_);
                auto attributesDict = FromProto(portalExitInfo.effective_inheritable_attributes());
                inheritableAttributes->MergeFrom(*attributesDict);

                const auto& securityManager = Bootstrap_->GetSecurityManager();
                auto effectiveAcl = DeserializeAclOrAlert(
                    ConvertToNode(TYsonString(portalExitInfo.effective_acl())),
                    securityManager);
                auto directAcl = DeserializeAclOrAlert(
                    ConvertToNode(TYsonString(portalExitInfo.direct_acl())),
                    securityManager);
                auto* owner = DeserializeOwner(portalExitInfo.owner());

                struct TAnnotationInfo {
                    TString Annotation;
                    TString Path;
                };
                std::optional<TAnnotationInfo> effectiveAnnotationInfo;
                if (portalExitInfo.has_effective_annotation()) {
                    effectiveAnnotationInfo = {
                        .Annotation = portalExitInfo.effective_annotation().annotation(),
                        .Path = portalExitInfo.effective_annotation().annotation_path(),
                    };
                }

                // NB: Fields of exitNode are updated after protobuf parsing in order to avoid partial updating.
                exitNode->EffectiveInheritableAttributes().emplace(inheritableAttributes->Attributes().ToPersistent());

                exitNode->Acd().SetEntries(effectiveAcl);
                exitNode->Acd().SetInherit(portalExitInfo.inherit_acl());
                exitNode->Acd().SetOwner(owner);

                exitNode->DirectAcd().SetEntries(directAcl);

                if (effectiveAnnotationInfo) {
                    exitNode->SetAnnotation(std::move(effectiveAnnotationInfo->Annotation));
                    exitNode->EffectiveAnnotationPath() = std::move(effectiveAnnotationInfo->Path);
                } else {
                    exitNode->RemoveAnnotation();
                    exitNode->EffectiveAnnotationPath().reset();
                }

                ++synchronizedPortalsCount;
            } catch (const std::exception& ex) {
                YT_LOG_ERROR(ex, "Portal exit synchronization failed (ExitNodeId: %v)",
                    nodeId);
                continue;
            }
        }

        YT_LOG_DEBUG("Portals were synchronized (SuccessCount: %v, FailureCount: %v)",
            synchronizedPortalsCount,
            request->portal_infos().size() - synchronizedPortalsCount);;
    }

    static void SanitizePortalExitExplicitAttributes(IAttributeDictionary* attributes)
    {
        for (auto attr : {
            EInternedAttributeKey::Acl,
            EInternedAttributeKey::Annotation,
            EInternedAttributeKey::InheritAcl,
            EInternedAttributeKey::Owner})
        {
            attributes->Remove(attr.Unintern());
        }
    }

    void HydraCreatePortalExit(NProto::TReqCreatePortalExit* request)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        auto entranceNodeId = FromProto<TObjectId>(request->entrance_node_id());
        auto accountId = FromProto<TAccountId>(request->account_id());

        auto effectiveAnnotation = request->has_effective_annotation()
            ? std::optional(request->effective_annotation())
            : std::nullopt;

        const auto& securityManager = Bootstrap_->GetSecurityManager();
        auto* account = securityManager->GetAccountOrThrow(accountId);

        auto effectiveAcl = DeserializeAclOrAlert(
            ConvertToNode(TYsonString(request->effective_acl())),
            securityManager);
        auto directAcl = request->has_direct_acl()
            ? std::optional(DeserializeAclOrAlert(
                ConvertToNode(TYsonString(request->direct_acl())),
                securityManager))
            : std::nullopt;

        auto explicitAttributes = FromProto(request->explicit_node_attributes());
        auto inheritedAttributes = FromProto(request->inherited_node_attributes());

        const auto& path = request->path();

        std::optional<NYPath::TYPath> effectiveAnnotationPath;
        if (request->has_effective_annotation_path()) {
            effectiveAnnotationPath = request->effective_annotation_path();
        } else if (effectiveAnnotation) {
            effectiveAnnotationPath = path;
        }

        const auto& multicellManager = Bootstrap_->GetMulticellManager();
        auto exitNodeId = MakePortalExitNodeId(entranceNodeId, multicellManager->GetCellTag());
        auto shardId = MakeCypressShardId(exitNodeId);

        const auto& cypressManager = Bootstrap_->GetCypressManager();
        auto* shard = cypressManager->CreateShard(shardId);

        TPortalExitNode* node;
        const auto& handler = cypressManager->GetHandler(EObjectType::PortalExit);

        auto effectiveInheritableAttributes = New<TInheritedAttributeDictionary>(Bootstrap_);
        if (inheritedAttributes) {
            effectiveInheritableAttributes->MergeFrom(*inheritedAttributes);
        } else {
            effectiveInheritableAttributes.Reset();
        }

        node = cypressManager->CreateNode(
            handler,
            exitNodeId,
            TCreateNodeContext{
                .ExternalCellTag = NotReplicatedCellTagSentinel,
                .InheritedAttributes = inheritedAttributes.Get(),
                .ExplicitAttributes = explicitAttributes.Get(),
                .Account = account,
                .Shard = shard
            })->As<TPortalExitNode>();

        node->SetParentId(FromProto<TNodeId>(request->parent_id()));
        if (request->has_key()) {
            node->SetKey(request->key());
        }

        cypressManager->SetShard(node, shard);
        shard->SetRoot(node);

        node->SetEntranceCellTag(CellTagFromId(entranceNodeId));

        node->EffectiveAnnotationPath() = std::move(effectiveAnnotationPath);
        if (effectiveInheritableAttributes) {
            node->EffectiveInheritableAttributes().emplace(effectiveInheritableAttributes->Attributes().ToPersistent());
        }

        const auto& objectManager = Bootstrap_->GetObjectManager();
        objectManager->RefObject(node);

        node->Acd().SetEntries(effectiveAcl);
        node->Acd().SetInherit(request->inherit_acl());

        if (directAcl) {
            node->DirectAcd().SetEntries(*directAcl);
        }

        if (auto ownerName = explicitAttributes->FindAndRemove<TString>(EInternedAttributeKey::Owner.Unintern())) {
            if (auto* owner = DeserializeOwner(*ownerName)) {
                node->Acd().SetOwner(owner);
            }
        }

        SanitizePortalExitExplicitAttributes(explicitAttributes.Get());
        try {
            handler->FillAttributes(node, inheritedAttributes.Get(), explicitAttributes.Get());
        } catch (const std::exception&) {
            // Unfortunately, we cannot cancel portal exit creation, so we just leave it with incorrect attributes.
            YT_LOG_ALERT("Invalid portal exit attributes; portal exit created with empty attributes (EntranceNodeId: %v, ExitNodeId: %v)",
                entranceNodeId,
                exitNodeId);
        }

        if (effectiveAnnotation) {
            node->SetAnnotation(*effectiveAnnotation);
        } else {
            node->RemoveAnnotation();
        }
        node->SetPath(path);

        shard->SetName(SuggestCypressShardName(shard));

        EmplaceOrCrash(ExitNodes_, node->GetId(), node);

        YT_LOG_DEBUG("Portal exit registered (ExitNodeId: %v, ShardId: %v, Account: %v, Path: %v)",
            exitNodeId,
            shard->GetId(),
            account->GetName(),
            path);
    }

    void HydraRemovePortalEntrance(NProto::TReqRemovePortalEntrance* request)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        auto entranceNodeId = FromProto<TObjectId>(request->entrance_node_id());

        const auto& cypressManager = Bootstrap_->GetCypressManager();
        auto* node = cypressManager->FindNode(TVersionedObjectId(entranceNodeId));
        if (!IsObjectAlive(node)) {
            YT_LOG_DEBUG("Attempt to remove a non-existing portal entrance node (EntranceNodeId: %v)",
                entranceNodeId);
            return;
        }

        auto* entranceNode = node->As<TPortalEntranceNode>();
        if (entranceNode->GetRemovalStarted()) {
            YT_LOG_DEBUG("Attempt to remove a portal entrance node for which removal is already started (EntranceNodeId: %v)",
                entranceNodeId);
            return;
        }

        entranceNode->SetRemovalStarted(true);

        YT_LOG_DEBUG("Portal entrance removal started (EntranceNodeId: %v)",
            entranceNodeId);

        // XXX(babenko)
        auto* parentNode = entranceNode->GetParent();
        auto entranceProxy = cypressManager->GetNodeProxy(entranceNode);
        auto parentProxy = cypressManager->GetNodeProxy(parentNode)->AsComposite();
        parentProxy->RemoveChild(entranceProxy);
    }

    void HydraRemovePortalExit(NProto::TReqRemovePortalExit* request)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        auto exitNodeId = FromProto<TObjectId>(request->node_id());

        const auto& cypressManager = Bootstrap_->GetCypressManager();
        auto* node = cypressManager->FindNode(TVersionedObjectId(exitNodeId));
        if (!IsObjectAlive(node)) {
            YT_LOG_DEBUG("Attempt to remove a non-existing portal exit node (ExitNodeId: %v)",
                exitNodeId);
            return;
        }

        auto* exitNode = node->As<TPortalExitNode>();
        if (exitNode->GetRemovalStarted()) {
            YT_LOG_DEBUG("Attempt to remove a portal exit node for which removal is already started (EntranceNodeId: %v)",
                exitNodeId);
            return;
        }

        exitNode->SetRemovalStarted(true);

        YT_LOG_DEBUG("Portal exit removal started (EntranceNodeId: %)",
            exitNodeId);

        const auto& objectManager = Bootstrap_->GetObjectManager();
        objectManager->UnrefObject(exitNode);
    }
};

////////////////////////////////////////////////////////////////////////////////

IPortalManagerPtr CreatePortalManager(TBootstrap* bootstrap)
{
    return New<TPortalManager>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressServer
