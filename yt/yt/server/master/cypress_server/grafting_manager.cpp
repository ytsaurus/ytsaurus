#include "grafting_manager.h"

#include "rootstock_node.h"
#include "scion_node.h"
#include "helpers.h"

#include <yt/yt/server/master/cypress_server/proto/grafting_manager.pb.h>

#include <yt/yt/server/master/cell_master/automaton.h>
#include <yt/yt/server/master/cell_master/config_manager.h>
#include <yt/yt/server/master/cell_master/hydra_facade.h>

#include <yt/yt/server/master/cypress_server/cypress_manager.h>

#include <yt/yt/server/master/security_server/helpers.h>

#include <yt/yt/server/master/transaction_server/public.h>

#include <yt/yt/server/lib/misc/interned_attributes.h>

#include <yt/yt/ytlib/cypress_client/cypress_ypath_proxy.h>

#include <yt/yt/core/yson/protobuf_helpers.h>

namespace NYT::NCypressServer {

using namespace NCellMaster;
using namespace NConcurrency;
using namespace NCypressClient;
using namespace NHydra;
using namespace NObjectClient;
using namespace NObjectServer;
using namespace NSecurityClient;
using namespace NSecurityServer;
using namespace NSequoiaServer;
using namespace NTransactionServer;
using namespace NTransactionSupervisor;
using namespace NYson;
using namespace NYTree;
using namespace NServer;

using NYT::ToProto;
using NCypressClient::NProto::TReqCreateRootstock;

////////////////////////////////////////////////////////////////////////////////

constinit const auto Logger = CypressServerLogger;

////////////////////////////////////////////////////////////////////////////////

class TGraftingManager
    : public IGraftingManager
    , public TMasterAutomatonPart
{
public:
    explicit TGraftingManager(TBootstrap* bootstrap)
        : TMasterAutomatonPart(
            bootstrap,
            EAutomatonThreadQueue::GraftingManager)
    {
        RegisterLoader(
            "GraftingManager.Keys",
            BIND_NO_PROPAGATE(&TGraftingManager::LoadKeys, Unretained(this)));
        RegisterLoader(
            "GraftingManager.Values",
            BIND_NO_PROPAGATE(&TGraftingManager::LoadValues, Unretained(this)));

        RegisterSaver(
            ESyncSerializationPriority::Keys,
            "GraftingManager.Keys",
            BIND_NO_PROPAGATE(&TGraftingManager::SaveKeys, Unretained(this)));
        RegisterSaver(
            ESyncSerializationPriority::Values,
            "GraftingManager.Values",
            BIND_NO_PROPAGATE(&TGraftingManager::SaveValues, Unretained(this)));

        RegisterMethod(BIND_NO_PROPAGATE(&TGraftingManager::HydraCreateScion, Unretained(this)));
        RegisterMethod(BIND_NO_PROPAGATE(&TGraftingManager::HydraSynchronizeScions, Unretained(this)));

        const auto& configManager = Bootstrap_->GetConfigManager();
        configManager->SubscribeConfigChanged(BIND_NO_PROPAGATE(&TGraftingManager::OnDynamicConfigChanged, MakeWeak(this)));
    }

    void Initialize() override
    {
        const auto& transactionManager = Bootstrap_->GetTransactionManager();
        transactionManager->RegisterTransactionActionHandlers<TReqCreateRootstock>({
            .Prepare = BIND_NO_PROPAGATE(&TGraftingManager::HydraCreateRootstock, Unretained(this)),
        });

        SynchronizeScionsExecutor_ = New<NConcurrency::TPeriodicExecutor>(
            Bootstrap_->GetHydraFacade()->GetAutomatonInvoker(NCellMaster::EAutomatonThreadQueue::GraftingManager),
            BIND(&TGraftingManager::OnSynchronizeScions, MakeWeak(this)));
        SynchronizeScionsExecutor_->Start();
    }

    void OnRootstockCreated(
        TRootstockNode* rootstockNode,
        const IAttributeDictionary& inheritedAttributes,
        const IAttributeDictionary& explicitAttributes) override
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(HasMutationContext());

        EmplaceOrCrash(RootstockNodes_, rootstockNode->GetId(), rootstockNode);

        PostScionCreationMessage(rootstockNode, inheritedAttributes, explicitAttributes);
    }

    void OnRootstockDestroyed(TRootstockNode* rootstockNode) override
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(HasMutationContext());

        if (RootstockNodes_.erase(rootstockNode->GetId()) != 1) {
            YT_LOG_DEBUG(
                "Unknown rootstock destroyed, ignored (RootstockNodeId: %v, ScionNodeId: %v)",
                rootstockNode->GetId(),
                rootstockNode->GetScionId());
            return;
        }

        auto scionNodeId = rootstockNode->GetScionId();

        YT_LOG_DEBUG(
            "Rootstock unregistered (RootstockNodeId: %v, ScionNodeId: %v)",
            rootstockNode->GetId(),
            scionNodeId);
    }

    void OnScionDestroyed(TScionNode* scionNode) override
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(HasMutationContext());

        if (ScionNodes_.erase(scionNode->GetId()) != 1) {
            YT_LOG_DEBUG(
                "Unknown scion destroyed, ignored (ScionNodeId: %v, RootstockNodeId: %v)",
                scionNode->GetId(),
                scionNode->GetRootstockId());
            return;
        }

        auto rootstockNodeId = scionNode->GetRootstockId();
        auto rootstockCellTag = CellTagFromId(rootstockNodeId);
        YT_VERIFY(rootstockCellTag == Bootstrap_->GetPrimaryCellTag());

        YT_LOG_DEBUG(
            "Scion unregistered (ScionNodeId: %v, RootstockNodeId: %v)",
            scionNode->GetId(),
            scionNode->GetRootstockId());
    }

    const TRootstockNodeMap& RootstockNodes() override
    {
        VerifyPersistentStateRead();

        return RootstockNodes_;
    }

    const TScionNodeMap& ScionNodes() override
    {
        VerifyPersistentStateRead();

        return ScionNodes_;
    }

private:
    DECLARE_THREAD_AFFINITY_SLOT(AutomatonThread);

    TRootstockNodeMap RootstockNodes_;
    TScionNodeMap ScionNodes_;

    NConcurrency::TPeriodicExecutorPtr SynchronizeScionsExecutor_;

    void OnDynamicConfigChanged(TDynamicClusterConfigPtr /*oldConfig*/)
    {
        const auto& configManager = Bootstrap_->GetConfigManager();
        const auto& config = configManager->GetConfig()->CypressManager;
        SynchronizeScionsExecutor_->SetPeriod(config->GraftSynchronizationPeriod);
    }

    void SaveKeys(NCellMaster::TSaveContext& /*context*/) const
    { }

    void SaveValues(NCellMaster::TSaveContext& context) const
    {
        using NYT::Save;

        Save(context, RootstockNodes_);
        Save(context, ScionNodes_);
    }

    void LoadKeys(NCellMaster::TLoadContext& /*context*/)
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);
    }

    void LoadValues(NCellMaster::TLoadContext& context)
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

        using NYT::Load;

        Load(context, RootstockNodes_);
        Load(context, ScionNodes_);
    }

    void Clear() override
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

        TMasterAutomatonPart::Clear();

        RootstockNodes_.clear();
        ScionNodes_.clear();
    }

    void OnSynchronizeScions()
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

        const auto& configManager = Bootstrap_->GetConfigManager();
        if (!configManager->GetConfig()->CypressManager->EnableScionSynchronization) {
            return;
        }

        if (!IsLeader()) {
            return;
        }

        const auto& multicellManager = Bootstrap_->GetMulticellManager();
        const auto& securityManager = Bootstrap_->GetSecurityManager();

        THashMap<TCellTag, std::vector<TRootstockNode*>> scionsByCellTag;

        for (auto [nodeId, node] : RootstockNodes_) {
            scionsByCellTag[CellTagFromId(node->GetScionId())].push_back(node);
        }

        for (auto [scionCellTag, scion] : scionsByCellTag) {
            NProto::TReqSynchronizeScions request;

            for (auto* node : scion) {
                YT_VERIFY(node->IsTrunk());
                YT_VERIFY(CellTagFromId(node->GetScionId()) == scionCellTag);

                const auto& cypressManager = Bootstrap_->GetCypressManager();

                auto* scionInfo = request.add_scion_infos();
                ToProto(scionInfo->mutable_node_id(), node->GetScionId());

                // Cf. #TPortalManager::OnSynchronizePortalExits.

                auto effectiveInheritableAttributes = New<TInheritedAttributeDictionary>(Bootstrap_);
                GatherInheritableAttributes(node->GetParent(), &effectiveInheritableAttributes->MutableAttributes());
                ToProto(scionInfo->mutable_effective_inheritable_attributes(), *effectiveInheritableAttributes);

                auto effectiveAcl = securityManager->GetEffectiveAcl(node);
                auto serializedEffectiveAcl = ConvertToYsonString(effectiveAcl).ToString();
                scionInfo->set_effective_acl(serializedEffectiveAcl);

                auto acd = securityManager->GetAcd(node);
                scionInfo->set_direct_acl(ToProto(ConvertToYsonString(acd->Acl())));
                scionInfo->set_inherit_acl(acd->Inherit());
                scionInfo->set_owner(ToProto(acd->GetOwner()->GetName()));

                if (auto annotationNode = FindClosestAncestorWithAnnotation(node)) {
                    scionInfo->mutable_effective_annotation()->set_annotation(*annotationNode->TryGetAnnotation());
                    scionInfo->mutable_effective_annotation()->set_annotation_path(
                        cypressManager->GetNodePath(annotationNode, /*transaction*/ nullptr));
                }
            }

            // NB: We do not actually need to know if this message was delivered,
            // so we send this message with reliable = false in order to avoid mutation creating.
            multicellManager->PostToMaster(request, scionCellTag, /*reliable*/ false);
        }
    }

    TSubject* DeserializeOwner(const std::string& ownerName)
    {
        const auto& securityManager = Bootstrap_->GetSecurityManager();
        auto* owner = securityManager->FindSubjectByNameOrAlias(ownerName, /*activeLifeStageOnly*/ false);
        if (!owner) {
            YT_LOG_ALERT("Serialized subject is missing (SubjectNameOrAlias: %v)",
                ownerName);
        }

        return owner;
    }

    void HydraSynchronizeScions(NProto::TReqSynchronizeScions* request)
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

        int synchronizedScionCount = 0;

        for (const auto& scionInfo : request->scion_infos()) {
            auto nodeId = FromProto<TObjectId>(scionInfo.node_id());
            auto it = ScionNodes_.find(nodeId);
            if (it == ScionNodes_.end()) {
                YT_LOG_ERROR("Skipping unknown scion synchronization (NodeId: %v)",
                    nodeId);
                continue;
            }

            auto scionNode = it->second;

            try {
                auto inheritableAttributes = New<TInheritedAttributeDictionary>(Bootstrap_);
                auto attributesDict = FromProto(scionInfo.effective_inheritable_attributes());
                inheritableAttributes->MergeFrom(*attributesDict);

                const auto& securityManager = Bootstrap_->GetSecurityManager();
                auto effectiveAcl = DeserializeAclOrAlert(
                    ConvertToNode(TYsonString(scionInfo.effective_acl())),
                    securityManager);
                auto directAcl = DeserializeAclOrAlert(
                    ConvertToNode(TYsonString(scionInfo.direct_acl())),
                    securityManager);
                auto* owner = DeserializeOwner(scionInfo.owner());

                struct TAnnotationInfo
                {
                    std::string Annotation;
                    TYPath Path;
                };
                std::optional<TAnnotationInfo> effectiveAnnotationInfo;
                if (scionInfo.has_effective_annotation()) {
                    effectiveAnnotationInfo = {
                        .Annotation = scionInfo.effective_annotation().annotation(),
                        .Path = scionInfo.effective_annotation().annotation_path(),
                    };
                }

                // NB: Fields of exitNode are updated after protobuf parsing in order to avoid partial updating.
                scionNode->EffectiveInheritableAttributes().emplace(inheritableAttributes->Attributes().ToPersistent());

                {
                    auto acd = securityManager->GetAcd(scionNode).AsMutable();
                    acd->SetEntries(effectiveAcl);
                    acd->SetInherit(scionInfo.inherit_acl());
                    acd->SetOwner(owner);
                }
                scionNode->DirectAcd().SetEntries(directAcl);

                if (effectiveAnnotationInfo) {
                    scionNode->SetAnnotation(std::move(effectiveAnnotationInfo->Annotation));
                    scionNode->EffectiveAnnotationPath() = std::move(effectiveAnnotationInfo->Path);
                } else {
                    scionNode->RemoveAnnotation();
                    scionNode->EffectiveAnnotationPath().reset();
                }

                ++synchronizedScionCount;
            } catch (const std::exception& ex) {
                YT_LOG_ERROR(ex, "Scion synchronization failed (ScionId: %v)",
                    nodeId);
                continue;
            }
        }

        YT_LOG_DEBUG("Scions were synchronized (SuccessCount: %v, FailureCount: %v)",
            synchronizedScionCount,
            request->scion_infos().size() - synchronizedScionCount);;
    }

    void HydraCreateRootstock(
        TTransaction* /*transaction*/,
        TReqCreateRootstock* request,
        const NTransactionSupervisor::TTransactionPrepareOptions& options)
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(options.Persistent);
        YT_VERIFY(options.LatePrepare);

        auto req = TCypressYPathProxy::Create(request->path());
        req->CopyFrom(request->request());

        const auto& objectManager = Bootstrap_->GetObjectManager();
        const auto& rootService = objectManager->GetRootService();
        // TODO(danilalexeev): YT-24358. Support permission validation.
        auto rsp = SyncExecuteVerb(rootService, req);
        auto rootstockNodeId = FromProto<TNodeId>(rsp->node_id());

        const auto& cypressManager = Bootstrap_->GetCypressManager();
        // TODO: Support creation in transaction.
        auto* rootstockNode = cypressManager
            ->GetNode(TVersionedNodeId(rootstockNodeId))
            ->As<TRootstockNode>();
        YT_VERIFY(rootstockNode->GetId() == rootstockNodeId);

        YT_LOG_DEBUG(
            "Rootstock created (RootstockId: %v, ScionId: %v)",
            rootstockNode->GetId(),
            rootstockNode->GetScionId());
    }

    // NB: This function should not throw since rootstock is already created.
    void PostScionCreationMessage(
        TRootstockNode* rootstockNode,
        const IAttributeDictionary& inheritedAttributes,
        const IAttributeDictionary& explicitAttributes) noexcept
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(HasMutationContext());

        auto* trunkNode = rootstockNode->GetTrunkNode()->As<TRootstockNode>();
        auto* transaction = rootstockNode->GetTransaction();

        NProto::TReqCreateScion request;
        ToProto(request.mutable_scion_node_id(), rootstockNode->GetScionId());
        ToProto(request.mutable_rootstock_node_id(), rootstockNode->GetId());
        ToProto(request.mutable_account_id(), rootstockNode->Account()->GetId());
        ToProto(request.mutable_explicit_node_attributes(), explicitAttributes);
        ToProto(request.mutable_inherited_node_attributes(), inheritedAttributes);

        const auto& cypressManager = Bootstrap_->GetCypressManager();
        auto path = cypressManager->GetNodePath(trunkNode, transaction);
        request.set_path(path);

        if (auto key = FindNodeKey(cypressManager, trunkNode, transaction)) {
            request.set_key(ToProto(*key));
        }

        const auto& securityManager = Bootstrap_->GetSecurityManager();
        auto effectiveAcl = securityManager->GetEffectiveAcl(trunkNode);
        request.set_effective_acl(ToProto(ConvertToYsonString(effectiveAcl)));

        const auto& directAcd = trunkNode->Acd();
        request.set_direct_acl(ToProto(ConvertToYsonString(directAcd.Acl())));
        request.set_inherit_acl(directAcd.Inherit());

        if (auto effectiveAnnotation = GetEffectiveAnnotation(rootstockNode)) {
            auto* annotationNode = FindClosestAncestorWithAnnotation(rootstockNode);
            YT_VERIFY(annotationNode);

            request.set_effective_annotation(*effectiveAnnotation);
            auto annotationPath = cypressManager->GetNodePath(annotationNode, transaction);
            request.set_effective_annotation_path(annotationPath);
        }

        ToProto(request.mutable_parent_node_id(), GetNodeParentId(trunkNode));

        auto scionNodeId = rootstockNode->GetScionId();
        auto scionCellTag = CellTagFromId(scionNodeId);

        if (scionCellTag == Bootstrap_->GetCellTag()) {
            HydraCreateScion(&request);
        } else {
            const auto& multicellManager = Bootstrap_->GetMulticellManager();
            multicellManager->PostToMaster(request, scionCellTag);
        }
    }

    static void SanitizeScionExplicitAttributes(IAttributeDictionary* attributes)
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

    void HydraCreateScion(NProto::TReqCreateScion* request)
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(HasMutationContext());

        auto rootstockNodeId = FromProto<TNodeId>(request->rootstock_node_id());
        auto scionNodeId = FromProto<TNodeId>(request->scion_node_id());
        auto parentId = FromProto<TNodeId>(request->parent_node_id());

        const auto& securityManager = Bootstrap_->GetSecurityManager();
        auto accountId = FromProto<TAccountId>(request->account_id());
        auto* account = securityManager->GetAccount(accountId);

        auto explicitAttributes = FromProto(request->explicit_node_attributes());

        auto inheritedAttributes = FromProto(request->inherited_node_attributes());
        auto effectiveInheritableAttributes = New<TInheritedAttributeDictionary>(Bootstrap_);
        if (inheritedAttributes) {
            effectiveInheritableAttributes->MergeFrom(*inheritedAttributes);
        } else {
            effectiveInheritableAttributes.Reset();
        }

        const auto& path = request->path();
        const auto& key = request->key();

        auto effectiveAcl = DeserializeAclOrAlert(
            ConvertToNode(TYsonString(request->effective_acl())),
            securityManager);
        auto directAcl = request->has_direct_acl()
            ? std::optional(DeserializeAclOrAlert(
                ConvertToNode(TYsonString(request->direct_acl())),
                securityManager))
            : std::nullopt;
        auto inheritAcl = request->inherit_acl();

        auto effectiveAnnotation = request->has_effective_annotation()
            ? std::optional(request->effective_annotation())
            : std::nullopt;
        std::optional<NYPath::TYPath> effectiveAnnotationPath;
        if (request->has_effective_annotation_path()) {
            effectiveAnnotationPath = request->effective_annotation_path();
        } else if (effectiveAnnotation) {
            effectiveAnnotationPath = path;
        }

        const auto& cypressManager = Bootstrap_->GetCypressManager();
        const auto& typeHandler = cypressManager->GetHandler(EObjectType::Scion);
        auto* shard = cypressManager->GetRootCypressShard();
        auto* scionNode = cypressManager->CreateNode(
            typeHandler,
            scionNodeId,
            TCreateNodeContext{
                .ExternalCellTag = NotReplicatedCellTagSentinel,
                .InheritedAttributes = inheritedAttributes.Get(),
                .ExplicitAttributes = explicitAttributes.Get(),
                .Account = account,
                .Shard = shard,
            })->As<TScionNode>();
        YT_VERIFY(scionNode->GetId() == scionNodeId);

        cypressManager->SetShard(scionNode, shard);

        if (effectiveInheritableAttributes) {
            scionNode->EffectiveInheritableAttributes().emplace(effectiveInheritableAttributes->Attributes().ToPersistent());
        }

        scionNode->ImmutableSequoiaProperties() = std::make_unique<TCypressNode::TImmutableSequoiaProperties>(
            key,
            path,
            parentId);
        scionNode->MutableSequoiaProperties() = std::make_unique<TCypressNode::TMutableSequoiaProperties>();

        {
            auto acd = securityManager->GetAcd(scionNode).AsMutable();
            acd->SetEntries(effectiveAcl);
            acd->SetInherit(inheritAcl);
            if (directAcl) {
                scionNode->DirectAcd().SetEntries(*directAcl);
            }

            if (auto ownerName = explicitAttributes->FindAndRemove<std::string>(EInternedAttributeKey::Owner.Unintern())) {
                if (auto* owner = securityManager->FindSubjectByNameOrAlias(*ownerName, /*activeLifeStageOnly*/ true)) {
                    acd->SetOwner(owner);
                } else {
                    YT_LOG_ALERT("Scion owner subject is missing (ScionNodeId: %v, SubjectName: %v)",
                        scionNode->GetId(),
                        ownerName);
                }
            }
        }

        SanitizeScionExplicitAttributes(explicitAttributes.Get());
        const auto& objectManager = Bootstrap_->GetObjectManager();
        objectManager->RefObject(scionNode);

        try {
            typeHandler->FillAttributes(scionNode, inheritedAttributes.Get(), explicitAttributes.Get());
        } catch (const std::exception& ex) {
            YT_LOG_ALERT(ex, "Failed to set scion attributes during creation "
                "(RootstockNodeId: %v, ScionNodeId: %v)",
                rootstockNodeId,
                scionNodeId);
        }

        if (effectiveAnnotation) {
            scionNode->SetAnnotation(*effectiveAnnotation);
        } else {
            scionNode->RemoveAnnotation();
        }
        scionNode->EffectiveAnnotationPath() = std::move(effectiveAnnotationPath);

        scionNode->SetRootstockId(rootstockNodeId);

        EmplaceOrCrash(ScionNodes_, scionNodeId, scionNode);

        typeHandler->SetReachable(scionNode);

        YT_LOG_DEBUG("Creating scion (ScionId: %v, RefCounter: %v)",
            scionNode->GetId(),
            scionNode->GetObjectRefCounter());

        YT_VERIFY(scionNode->GetObjectRefCounter() > 1);

        objectManager->UnrefObject(scionNode);

        YT_LOG_DEBUG("Scion created "
            "(RootstockNodeId: %v, ScionNodeId: %v)",
            rootstockNodeId,
            scionNodeId);
    }
};

////////////////////////////////////////////////////////////////////////////////

IGraftingManagerPtr CreateGraftingManager(TBootstrap* bootstrap)
{
    return New<TGraftingManager>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressServer
