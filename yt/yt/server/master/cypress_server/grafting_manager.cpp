#include "grafting_manager.h"

#include "rootstock_node.h"
#include "scion_node.h"
#include "helpers.h"

#include <yt/yt/server/master/cypress_server/proto/grafting_manager.pb.h>

#include <yt/yt/server/master/cell_master/automaton.h>
#include <yt/yt/server/master/cell_master/config_manager.h>
#include <yt/yt/server/master/cell_master/hydra_facade.h>

#include <yt/yt/server/master/security_server/helpers.h>

#include <yt/yt/server/master/transaction_server/public.h>

#include <yt/yt/server/lib/misc/interned_attributes.h>

#include <yt/yt/ytlib/cypress_client/cypress_ypath_proxy.h>

namespace NYT::NCypressServer {

using namespace NCellMaster;
using namespace NConcurrency;
using namespace NCypressClient;
using namespace NHydra;
using namespace NObjectClient;
using namespace NSecurityClient;
using namespace NSecurityServer;
using namespace NSequoiaServer;
using namespace NTransactionServer;
using namespace NTransactionSupervisor;
using namespace NYson;
using namespace NYTree;

using NCypressClient::NProto::TReqCreateRootstock;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = CypressServerLogger;

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
            BIND(&TGraftingManager::LoadKeys, Unretained(this)));
        RegisterLoader(
            "GraftingManager.Values",
            BIND(&TGraftingManager::LoadValues, Unretained(this)));

        RegisterSaver(
            ESyncSerializationPriority::Keys,
            "GraftingManager.Keys",
            BIND(&TGraftingManager::SaveKeys, Unretained(this)));
        RegisterSaver(
            ESyncSerializationPriority::Values,
            "GraftingManager.Values",
            BIND(&TGraftingManager::SaveValues, Unretained(this)));

        RegisterMethod(BIND(&TGraftingManager::HydraCreateScion, Unretained(this)));
    }

    void Initialize() override
    {
        const auto& transactionManager = Bootstrap_->GetTransactionManager();
        transactionManager->RegisterTransactionActionHandlers<TReqCreateRootstock>({
            .Prepare = BIND_NO_PROPAGATE(&TGraftingManager::HydraCreateRootstock, Unretained(this)),
        });
    }

    void OnRootstockCreated(
        TRootstockNode* rootstockNode,
        const IAttributeDictionary& inheritedAttributes,
        const IAttributeDictionary& explicitAttributes) override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(HasMutationContext());

        EmplaceOrCrash(RootstockNodes_, rootstockNode->GetId(), rootstockNode);

        PostScionCreationMessage(rootstockNode, inheritedAttributes, explicitAttributes);
    }

    void OnRootstockDestroyed(TRootstockNode* rootstockNode) override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
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
        VERIFY_THREAD_AFFINITY(AutomatonThread);
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
        Bootstrap_->VerifyPersistentStateRead();

        return RootstockNodes_;
    }

    const TScionNodeMap& ScionNodes() override
    {
        Bootstrap_->VerifyPersistentStateRead();

        return ScionNodes_;
    }

private:
    DECLARE_THREAD_AFFINITY_SLOT(AutomatonThread);

    TRootstockNodeMap RootstockNodes_;
    TScionNodeMap ScionNodes_;

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
        VERIFY_THREAD_AFFINITY(AutomatonThread);
    }

    void LoadValues(NCellMaster::TLoadContext& context)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        using NYT::Load;

        Load(context, RootstockNodes_);
        Load(context, ScionNodes_);
        // COMPAT(kvk1920)
        if (context.GetVersion() < EMasterReign::SequoiaMapNode) {
            Load<THashSet<TNodeId>>(context);
        }
    }

    void Clear() override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        TMasterAutomatonPart::Clear();

        RootstockNodes_.clear();
        ScionNodes_.clear();
    }

    void HydraCreateRootstock(
        TTransaction* /*transaction*/,
        TReqCreateRootstock* request,
        const NTransactionSupervisor::TTransactionPrepareOptions& options)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(options.Persistent);
        YT_VERIFY(options.LatePrepare);

        auto req = TCypressYPathProxy::Create(request->path());
        req->CopyFrom(request->request());

        const auto& objectManager = Bootstrap_->GetObjectManager();
        const auto& rootService = objectManager->GetRootService();
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
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(HasMutationContext());

        auto* trunkNode = rootstockNode->GetTrunkNode()->As<TScionNode>();
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
            request.set_key(*key);
        }

        const auto& securityManager = Bootstrap_->GetSecurityManager();
        auto effectiveAcl = securityManager->GetEffectiveAcl(trunkNode);
        request.set_effective_acl(ConvertToYsonString(effectiveAcl).ToString());

        const auto& directAcd = trunkNode->Acd();
        request.set_direct_acl(ConvertToYsonString(directAcd.Acl()).ToString());
        request.set_inherit_acl(directAcd.Inherit());

        if (auto effectiveAnnotation = GetEffectiveAnnotation(rootstockNode)) {
            auto* annotationNode = FindClosestAncestorWithAnnotation(rootstockNode);
            YT_VERIFY(annotationNode);

            request.set_effective_annotation(*effectiveAnnotation);
            auto annotationPath = cypressManager->GetNodePath(annotationNode, transaction);
            request.set_effective_annotation_path(annotationPath);
        }

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
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(HasMutationContext());

        auto rootstockNodeId = FromProto<TNodeId>(request->rootstock_node_id());
        auto scionNodeId = FromProto<TNodeId>(request->scion_node_id());

        const auto& securityManager = Bootstrap_->GetSecurityManager();
        auto accountId = FromProto<TAccountId>(request->account_id());
        auto* account = securityManager->GetAccountOrThrow(accountId);

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

        const auto& objectManager = Bootstrap_->GetObjectManager();
        objectManager->RefObject(scionNode);

        cypressManager->SetShard(scionNode, shard);

        if (effectiveInheritableAttributes) {
            scionNode->EffectiveInheritableAttributes().emplace(effectiveInheritableAttributes->Attributes().ToPersistent());
        }

        scionNode->ImmutableSequoiaProperties() = std::make_unique<TCypressNode::TImmutableSequoiaProperties>(key, path);
        scionNode->MutableSequoiaProperties() = std::make_unique<TCypressNode::TMutableSequoiaProperties>();

        scionNode->Acd().SetEntries(effectiveAcl);
        scionNode->Acd().SetInherit(inheritAcl);
        if (directAcl) {
            scionNode->DirectAcd().SetEntries(*directAcl);
        }

        if (auto ownerName = explicitAttributes->FindAndRemove<TString>(EInternedAttributeKey::Owner.Unintern())) {
            if (auto* owner = securityManager->FindSubjectByNameOrAlias(*ownerName, /*activeLifeStageOnly*/ true)) {
                scionNode->Acd().SetOwner(owner);
            } else {
                YT_LOG_ALERT("Scion owner subject is missing (ScionNodeId: %v, SubjectName: %v)",
                    scionNode->GetId(),
                    ownerName);
            }
        }

        SanitizeScionExplicitAttributes(explicitAttributes.Get());
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
