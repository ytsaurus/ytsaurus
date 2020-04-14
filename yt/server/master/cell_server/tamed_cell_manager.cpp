#include "bundle_node_tracker.h"
#include "config.h"
#include "private.h"
#include "cell_base.h"
#include "cell_bundle.h"
#include "cell_bundle_type_handler.h"
#include "cell_type_handler_base.h"
#include "tamed_cell_manager.h"
#include "cell_tracker.h"

#include <yt/server/master/tablet_server/tablet_cell_decommissioner.h>
#include <yt/server/master/tablet_server/cypress_integration.h>
#include <yt/server/master/tablet_server/tablet_manager.h>

#include <yt/server/master/cell_master/config.h>
#include <yt/server/master/cell_master/config_manager.h>
#include <yt/server/master/cell_master/bootstrap.h>
#include <yt/server/master/cell_master/hydra_facade.h>
#include <yt/server/master/cell_master/serialize.h>

#include <yt/server/master/chunk_server/chunk_list.h>
#include <yt/server/master/chunk_server/chunk_view.h>
#include <yt/server/master/chunk_server/chunk_manager.h>
#include <yt/server/master/chunk_server/chunk_tree_traverser.h>
#include <yt/server/master/chunk_server/helpers.h>
#include <yt/server/master/chunk_server/medium.h>

#include <yt/server/master/cypress_server/cypress_manager.h>

#include <yt/server/master/table_server/table_node.h>

#include <yt/server/lib/hive/hive_manager.h>
#include <yt/server/lib/hive/helpers.h>

#include <yt/server/lib/misc/interned_attributes.h>

#include <yt/server/master/node_tracker_server/node.h>
#include <yt/server/master/node_tracker_server/node_tracker.h>

#include <yt/server/master/object_server/object_manager.h>

#include <yt/server/master/security_server/security_manager.h>
#include <yt/server/master/security_server/group.h>
#include <yt/server/master/security_server/subject.h>

#include <yt/server/lib/tablet_server/proto/tablet_manager.pb.h>

#include <yt/server/lib/cell_server/proto/cell_manager.pb.h>

#include <yt/ytlib/election/config.h>

#include <yt/ytlib/hive/cell_directory.h>

#include <yt/client/object_client/helpers.h>

#include <yt/ytlib/cypress_client/cypress_ypath_proxy.h>

#include <yt/ytlib/tablet_client/config.h>

#include <yt/ytlib/transaction_client/helpers.h>

#include <yt/core/concurrency/periodic_executor.h>
#include <yt/core/concurrency/throughput_throttler.h>

#include <yt/core/misc/collection_helpers.h>
#include <yt/core/misc/random_access_queue.h>
#include <yt/core/misc/string.h>

#include <yt/core/ypath/token.h>

#include <algorithm>

namespace NYT::NCellServer {

using namespace NCellMaster;
using namespace NChunkClient::NProto;
using namespace NChunkClient;
using namespace NChunkServer;
using namespace NConcurrency;
using namespace NCypressClient;
using namespace NCypressServer;
using namespace NHiveClient;
using namespace NHiveServer;
using namespace NHydra;
using namespace NNodeTrackerClient::NProto;
using namespace NNodeTrackerClient;
using namespace NNodeTrackerServer::NProto;
using namespace NNodeTrackerServer;
using namespace NObjectClient::NProto;
using namespace NObjectClient;
using namespace NObjectServer;
using namespace NProfiling;
using namespace NSecurityServer;
using namespace NTableServer;
using namespace NTabletServer::NProto;
using namespace NTransactionClient;
using namespace NTransactionServer;
using namespace NYPath;
using namespace NYTree;
using namespace NYson;

using NNodeTrackerClient::TNodeDescriptor;
using NNodeTrackerServer::NProto::TReqIncrementalHeartbeat;
using NTransactionServer::TTransaction;

using NYT::FromProto;
using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = CellServerLogger;

static const auto ProfilingPeriod = TDuration::Seconds(10);

////////////////////////////////////////////////////////////////////////////////

class TTamedCellManager::TImpl
    : public TMasterAutomatonPart
{
public:
    DEFINE_SIGNAL(void(TCellBundle* bundle), CellBundleCreated);
    DEFINE_SIGNAL(void(TCellBundle* bundle), CellBundleDestroyed);
    DEFINE_SIGNAL(void(TCellBundle* bundle), CellBundleNodeTagFilterChanged);
    DEFINE_SIGNAL(void(TCellBase* cell), CellDecommissionStarted);
    DEFINE_SIGNAL(void(), CellPeersAssigned);
    DEFINE_SIGNAL(void(), AfterSnapshotLoaded);

public:
    explicit TImpl(NCellMaster::TBootstrap* bootstrap)
        : TMasterAutomatonPart(bootstrap, NCellMaster::EAutomatonThreadQueue::TamedCellManager)
        , CellTracker_(New<TCellTracker>(Bootstrap_))
        , BundleNodeTracker_(New<TBundleNodeTracker>(Bootstrap_))
        , CellBundleMap_(TEntityMapTypeTraits<TCellBundle>(Bootstrap_))
        , CellMap_(TEntityMapTypeTraits<TCellBase>(Bootstrap_))
    {
        VERIFY_INVOKER_THREAD_AFFINITY(
            Bootstrap_->GetHydraFacade()->GetAutomatonInvoker(EAutomatonThreadQueue::Default), AutomatonThread);

        RegisterLoader(
            "CellManager.Keys",
            BIND(&TImpl::LoadKeys, Unretained(this)));
        RegisterLoader(
            "CellManager.Values",
            BIND(&TImpl::LoadValues, Unretained(this)));

        RegisterSaver(
            ESyncSerializationPriority::Keys,
            "CellManager.Keys",
            BIND(&TImpl::SaveKeys, Unretained(this)));
        RegisterSaver(
            ESyncSerializationPriority::Values,
            "CellManager.Values",
            BIND(&TImpl::SaveValues, Unretained(this)));

        RegisterMethod(BIND(&TImpl::HydraAssignPeers, Unretained(this)));
        RegisterMethod(BIND(&TImpl::HydraRevokePeers, Unretained(this)));
        RegisterMethod(BIND(&TImpl::HydraReassignPeers, Unretained(this)));
        RegisterMethod(BIND(&TImpl::HydraSetLeadingPeer, Unretained(this)));
        RegisterMethod(BIND(&TImpl::HydraStartPrerequisiteTransaction, Unretained(this)));
        RegisterMethod(BIND(&TImpl::HydraAbortPrerequisiteTransaction, Unretained(this)));
        RegisterMethod(BIND(&TImpl::HydraDecommissionCellOnMaster, Unretained(this)));
        RegisterMethod(BIND(&TImpl::HydraOnCellDecommissionedOnNode, Unretained(this)));
        RegisterMethod(BIND(&TImpl::HydraOnCellDecommissionedOnMaster, Unretained(this)));
        RegisterMethod(BIND(&TImpl::HydraSetCellConfigVersion, Unretained(this)));
        RegisterMethod(BIND(&TImpl::HydraSetCellStatus, Unretained(this)));
        RegisterMethod(BIND(&TImpl::HydraUpdateCellHealth, Unretained(this)));
        RegisterMethod(BIND(&TImpl::HydraUpdatePeerCount, Unretained(this)));
    }

    void Initialize()
    {
        const auto& nodeTracker = Bootstrap_->GetNodeTracker();
        nodeTracker->SubscribeIncrementalHeartbeat(BIND(&TImpl::OnIncrementalHeartbeat, MakeWeak(this)));
        nodeTracker->SubscribeNodeUnregistered(BIND(&TImpl::OnNodeUnregistered, MakeWeak(this)));

        const auto& configManager = Bootstrap_->GetConfigManager();
        configManager->SubscribeConfigChanged(BIND(&TImpl::OnDynamicConfigChanged, MakeWeak(this)));

        const auto& transactionManager = Bootstrap_->GetTransactionManager();
        transactionManager->SubscribeTransactionCommitted(BIND(&TImpl::OnTransactionFinished, MakeWeak(this)));
        transactionManager->SubscribeTransactionAborted(BIND(&TImpl::OnTransactionFinished, MakeWeak(this)));

        const auto& multicellManager = Bootstrap_->GetMulticellManager();
        if (multicellManager->IsPrimaryMaster()) {
            multicellManager->SubscribeReplicateKeysToSecondaryMaster(
                BIND(&TImpl::OnReplicateKeysToSecondaryMaster, MakeWeak(this)));
            multicellManager->SubscribeReplicateValuesToSecondaryMaster(
                BIND(&TImpl::OnReplicateValuesToSecondaryMaster, MakeWeak(this)));
        }

        BundleNodeTracker_->Initialize();

        ProfilingExecutor_ = New<TPeriodicExecutor>(
            Bootstrap_->GetHydraFacade()->GetAutomatonInvoker(EAutomatonThreadQueue::Periodic),
            BIND(&TImpl::OnProfiling, MakeWeak(this)),
            ProfilingPeriod);
        ProfilingExecutor_->Start();
    }

    TCellBundle* CreateCellBundle(
        const TString& name,
        std::unique_ptr<TCellBundle> holder,
        TTabletCellOptionsPtr options)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        ValidateCellBundleName(name);

        if (FindCellBundleByName(name)) {
            THROW_ERROR_EXCEPTION(
                NYTree::EErrorCode::AlreadyExists,
                "Cell bundle %Qv already exists",
                name);
        }

        holder->SetName(name);

        auto id = holder->GetId();
        auto* cellBundle = CellBundleMap_.Insert(id, std::move(holder));
        YT_VERIFY(NameToCellBundleMap_.insert(std::make_pair(cellBundle->GetName(), cellBundle)).second);
        cellBundle->SetOptions(std::move(options));

        const auto& objectManager = Bootstrap_->GetObjectManager();
        objectManager->RefObject(cellBundle);

        CellBundleCreated_.Fire(cellBundle);

        return cellBundle;
    }

    void ZombifyCellBundle(TCellBundle* cellBundle)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        // Remove tablet cell bundle from maps.
        YT_VERIFY(NameToCellBundleMap_.erase(cellBundle->GetName()) == 1);

        CellBundleDestroyed_.Fire(cellBundle);
    }

    void DestroyCellBundle(TCellBundle* cellBundle)
    {
        CellBundleMap_.Release(cellBundle->GetId()).release();
    }

    void SetCellBundleOptions(TCellBundle* cellBundle, TTabletCellOptionsPtr options)
    {
        if (options->PeerCount != cellBundle->GetOptions()->PeerCount && !cellBundle->Cells().empty()) {
            THROW_ERROR_EXCEPTION("Cannot change peer count since tablet cell bundle has %v tablet cell(s)",
                                  cellBundle->Cells().size());
        }

        auto snapshotAcl = ConvertToYsonString(options->SnapshotAcl, EYsonFormat::Binary).GetData();
        auto changelogAcl = ConvertToYsonString(options->ChangelogAcl, EYsonFormat::Binary).GetData();

        cellBundle->SetOptions(std::move(options));

        for (auto* cell : GetValuesSortedByKey(cellBundle->Cells())) {
            const auto& multicellManager = Bootstrap_->GetMulticellManager();
            if (multicellManager->IsPrimaryMaster()) {
                if (auto node = FindCellNode(cell->GetId())) {
                    auto cellNode = node->AsMap();

                    try {
                        {
                            auto req = TCypressYPathProxy::Set("/snapshots/@acl");
                            req->set_value(snapshotAcl);
                            SyncExecuteVerb(cellNode, req);
                        }
                        {
                            auto req = TCypressYPathProxy::Set("/changelogs/@acl");
                            req->set_value(changelogAcl);
                            SyncExecuteVerb(cellNode, req);
                        }
                    } catch (const std::exception& ex) {
                        YT_LOG_ALERT_UNLESS(IsRecovery(), ex,
                            "Unexpected error: caught exception while changing ACL (Bundle: %v, TabletCellId: %v)",
                            cellBundle->GetName(),
                            cell->GetId());
                    }
                }

                RestartPrerequisiteTransaction(cell);
            }

            ReconfigureCell(cell);
        }
    }

    TCellBase* CreateCell(TCellBundle* cellBundle, std::unique_ptr<TCellBase> holder)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        const auto& securityManager = Bootstrap_->GetSecurityManager();
        securityManager->ValidatePermission(cellBundle, EPermission::Use);

        const auto& objectManager = Bootstrap_->GetObjectManager();

        holder->Peers().resize(cellBundle->GetOptions()->PeerCount);
        holder->SetCellBundle(cellBundle);
        YT_VERIFY(cellBundle->Cells().insert(holder.get()).second);
        objectManager->RefObject(cellBundle);

        ReconfigureCell(holder.get());

        auto id = holder->GetId();
        auto* cell = CellMap_.Insert(id, std::move(holder));

        // Make the fake reference.
        YT_VERIFY(cell->RefObject() == 1);

        cell->GossipStatus().Initialize(Bootstrap_);

        const auto& hiveManager = Bootstrap_->GetHiveManager();
        hiveManager->CreateMailbox(id);

        auto cellMapNodeProxy = GetCellMapNode();
        auto cellNodePath = "/" + ToString(id);

        try {
            // NB: Users typically are not allowed to create these types.
            auto* rootUser = securityManager->GetRootUser();
            TAuthenticatedUserGuard userGuard(securityManager, rootUser);

            // Create Cypress node.
            {
                auto req = TCypressYPathProxy::Create(cellNodePath);
                req->set_type(static_cast<int>(EObjectType::TabletCellNode));

                auto attributes = CreateEphemeralAttributes();
                attributes->Set("opaque", true);
                ToProto(req->mutable_node_attributes(), *attributes);

                SyncExecuteVerb(cellMapNodeProxy, req);
            }

            const auto& multicellManager = Bootstrap_->GetMulticellManager();
            if (multicellManager->IsPrimaryMaster()) {
                auto attributes = CreateEphemeralAttributes();
                attributes->Set("inherit_acl", false);

                // Create "snapshots" child.
                {
                    auto req = TCypressYPathProxy::Create(cellNodePath + "/snapshots");
                    req->set_type(static_cast<int>(EObjectType::MapNode));
                    attributes->Set("acl", cellBundle->GetOptions()->SnapshotAcl);
                    ToProto(req->mutable_node_attributes(), *attributes);

                    SyncExecuteVerb(cellMapNodeProxy, req);
                }

                // Create "changelogs" child.
                {
                    auto req = TCypressYPathProxy::Create(cellNodePath + "/changelogs");
                    req->set_type(static_cast<int>(EObjectType::MapNode));
                    attributes->Set("acl", cellBundle->GetOptions()->ChangelogAcl);
                    ToProto(req->mutable_node_attributes(), *attributes);

                    SyncExecuteVerb(cellMapNodeProxy, req);
                }
            }
        } catch (const std::exception& ex) {
            YT_LOG_ERROR_UNLESS(
                IsRecovery(),
                ex,
                "Error registering tablet cell in Cypress (CellId: %v)",
                cell->GetId());

            objectManager->UnrefObject(cell);
            THROW_ERROR_EXCEPTION("Error registering tablet cell in Cypress")
                    << ex;
        }

        return cell;
    }

    void ZombifyCell(TCellBase* cell)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        const auto& hiveManager = Bootstrap_->GetHiveManager();
        auto cellId = cell->GetId();
        auto* mailbox = hiveManager->FindMailbox(cellId);
        if (mailbox) {
            hiveManager->RemoveMailbox(mailbox);
        }

        for (const auto& peer : cell->Peers()) {
            if (peer.Node) {
                peer.Node->DetachTabletCell(cell);
            }
            if (!peer.Descriptor.IsNull()) {
                RemoveFromAddressToCellMap(peer.Descriptor, cell);
            }
        }
        cell->Peers().clear();

        auto* cellBundle = cell->GetCellBundle();
        YT_VERIFY(cellBundle->Cells().erase(cell) == 1);
        const auto& objectManager = Bootstrap_->GetObjectManager();
        objectManager->UnrefObject(cellBundle);
        cell->SetCellBundle(nullptr);

        // NB: Code below interacts with other master parts and may require root permissions
        // (for example, when aborting a transaction).
        // We want this code to always succeed.
        const auto& securityManager = Bootstrap_->GetSecurityManager();
        auto* rootUser = securityManager->GetRootUser();
        TAuthenticatedUserGuard userGuard(securityManager, rootUser);

        const auto& multicellManager = Bootstrap_->GetMulticellManager();
        if (multicellManager->IsPrimaryMaster()) {
            AbortPrerequisiteTransaction(cell);
            AbortCellSubtreeTransactions(cell);
        }

        auto cellNodeProxy = FindCellNode(cellId);
        if (cellNodeProxy) {
            try {
                // NB: Subtree transactions were already aborted in AbortPrerequisiteTransaction.
                cellNodeProxy->GetParent()->RemoveChild(cellNodeProxy);
            } catch (const std::exception& ex) {
                YT_LOG_ERROR_UNLESS(IsRecovery(), ex, "Error unregisterting tablet cell from Cypress");
            }
        }
    }

    void DestroyCell(TCellBase* cell)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        CellMap_.Release(cell->GetId()).release();
    }

    void UpdatePeerCount(TCellBase* cell, std::optional<int> peerCount)
    {
        cell->PeerCount() = peerCount;
        cell->LastPeerCountUpdateTime() = TInstant::Now();

        int oldPeerCount = static_cast<int>(cell->Peers().size());
        int newPeerCount = cell->GetCellBundle()->GetOptions()->PeerCount;
        if (cell->PeerCount()) {
            newPeerCount = *cell->PeerCount();
        }

        if (oldPeerCount == newPeerCount) {
            return;
        }

        YT_LOG_DEBUG("Updating cell peer count (CellId: %v, OldPeerCount: %v, NewPeerCount: %v)",
            cell->GetId(),
            oldPeerCount,
            newPeerCount);

        bool leaderChanged = false;
        if (newPeerCount > oldPeerCount) {
            cell->Peers().resize(newPeerCount);
        } else {
            // Move leader to the first place to prevent its removing.
            int leaderId = cell->GetLeadingPeerId();
            if (leaderId != 0) {
                leaderChanged = true;
                RemoveFromAddressToCellMap(cell->Peers()[leaderId].Descriptor, cell);
                RemoveFromAddressToCellMap(cell->Peers()[0].Descriptor, cell);
                std::swap(cell->Peers()[cell->GetLeadingPeerId()], cell->Peers()[0]);
                AddToAddressToCellMap(cell->Peers()[leaderId].Descriptor, cell, leaderId);
                AddToAddressToCellMap(cell->Peers()[0].Descriptor, cell, 0);
                cell->SetLeadingPeerId(0);
            }

            // Revoke extra peers.
            for (int revokingPeerIndex = newPeerCount; revokingPeerIndex < oldPeerCount; ++revokingPeerIndex) {
                DoRevokePeer(cell, revokingPeerIndex);
            }

            cell->Peers().resize(newPeerCount);
        }

        const auto& multicellManager = Bootstrap_->GetMulticellManager();
        if (leaderChanged && multicellManager->IsPrimaryMaster()) {
            RestartPrerequisiteTransaction(cell);
        }

        ReconfigureCell(cell);

        // Notify new quorum as soon as possible via heartbeat requests.
        if (multicellManager->IsPrimaryMaster() && IsLeader()) {
            for (const auto& peer : cell->Peers()) {
                if (peer.Node) {
                    Bootstrap_->GetNodeTracker()->RequestNodeHeartbeat(peer.Node->GetId());
                }
            }
        }
    }

    const TCellSet* FindAssignedCells(const TString& address) const
    {
        auto it = AddressToCell_.find(address);
        return it != AddressToCell_.end()
           ? &it->second
           : nullptr;
    }

    const TBundleNodeTrackerPtr& GetBundleNodeTracker()
    {
        return BundleNodeTracker_;
    }

    TCellBase* GetCellOrThrow(TTamedCellId id)
    {
        auto* cell = FindCell(id);
        if (!IsObjectAlive(cell)) {
            THROW_ERROR_EXCEPTION(
                NYTree::EErrorCode::ResolveError,
                "No such tablet cell %v",
                id);
        }
        return cell;
    }

    void RemoveCell(TCellBase* cell, bool force)
    {
        const auto& multicellManager = Bootstrap_->GetMulticellManager();
        YT_VERIFY(multicellManager->IsPrimaryMaster());

        YT_LOG_DEBUG_UNLESS(IsRecovery(), "Removing tablet cell (CellId: %v, Force: %v)",
            cell->GetId(),
            force);

        switch (cell->GetCellLifeStage()) {
            case ECellLifeStage::Running: {
                // Decommission tablet cell on primary master.
                DecommissionCell(cell);

                // Decommission tablet cell on secondary masters.
                NTabletServer::NProto::TReqDecommissionTabletCellOnMaster req;
                ToProto(req.mutable_cell_id(), cell->GetId());
                multicellManager->PostToMasters(req, multicellManager->GetRegisteredMasterCellTags());

                // Decommission tablet cell on node.
                if (force) {
                    OnCellDecommissionedOnNode(cell);
                }

                break;
            }

            case ECellLifeStage::DecommissioningOnMaster:
            case ECellLifeStage::DecommissioningOnNode:
                if (force) {
                    OnCellDecommissionedOnNode(cell);
                }

                break;

            default:
                YT_ABORT();
        }
    }

    void HydraOnCellDecommissionedOnMaster(TReqOnTabletCellDecommisionedOnMaster* request)
    {
        auto cellId = FromProto<TTamedCellId>(request->cell_id());
        auto* cell = FindCell(cellId);
        if (!IsObjectAlive(cell)) {
            return;
        }

        if (cell->GetCellLifeStage() != ECellLifeStage::DecommissioningOnMaster) {
            return;
        }

        // Decommission tablet cell on node.

        YT_LOG_DEBUG_UNLESS(IsRecovery(), "Requesting tablet cell decommission on node (CellId: %v)",
            cell->GetId());

        cell->SetCellLifeStage(ECellLifeStage::DecommissioningOnNode);

        const auto& hiveManager = Bootstrap_->GetHiveManager();
        auto* mailbox = hiveManager->GetMailbox(cell->GetId());
        hiveManager->PostMessage(mailbox, TReqDecommissionTabletCellOnNode());
    }

    void HydraDecommissionCellOnMaster(TReqDecommissionTabletCellOnMaster* request)
    {
        auto cellId = FromProto<TTamedCellId>(request->cell_id());
        auto* cell = FindCell(cellId);
        if (!IsObjectAlive(cell)) {
            return;
        }
        DecommissionCell(cell);
        OnCellDecommissionedOnNode(cell);
    }

    void HydraUpdatePeerCount(TReqUpdatePeerCount* request)
    {
        auto cellId = FromProto<TTamedCellId>(request->cell_id());
        auto* cell = FindCell(cellId);
        if (!IsObjectAlive(cell)) {
            return;
        }

        if (request->has_peer_count()) {
            UpdatePeerCount(cell, request->peer_count());
        } else {
            UpdatePeerCount(cell, std::nullopt);
        }

        const auto& multicellManager = Bootstrap_->GetMulticellManager();
        if (multicellManager->IsPrimaryMaster()) {
            multicellManager->PostToMasters(*request, multicellManager->GetRegisteredMasterCellTags());
        }
    }

    void DecommissionCell(TCellBase* cell)
    {
        if (cell->IsDecommissionStarted()) {
            return;
        }

        cell->SetCellLifeStage(ECellLifeStage::DecommissioningOnMaster);
        cell->GossipStatus().Local().Decommissioned = true;

        CellDecommissionStarted_.Fire(cell);
    }

    void HydraOnCellDecommissionedOnNode(TRspDecommissionTabletCellOnNode* response)
    {
        auto cellId = FromProto<TTamedCellId>(response->cell_id());
        auto* cell = FindCell(cellId);
        if (!IsObjectAlive(cell)) {
            return;
        }
        OnCellDecommissionedOnNode(cell);
    }

    void OnCellDecommissionedOnNode(TCellBase* cell)
    {
        if (cell->IsDecommissionCompleted()) {
            return;
        }

        cell->SetCellLifeStage(ECellLifeStage::Decommissioned);

        YT_LOG_DEBUG_UNLESS(IsRecovery(), "Tablet cell decommissioned (CellId: %v)",
            cell->GetId());
    }

    TCellBundle* FindCellBundleByName(const TString& name)
    {
        auto it = NameToCellBundleMap_.find(name);
        return it == NameToCellBundleMap_.end() ? nullptr : it->second;
    }

    TCellBundle* GetCellBundleByNameOrThrow(const TString& name)
    {
        auto* cellBundle = FindCellBundleByName(name);
        if (!cellBundle) {
            THROW_ERROR_EXCEPTION(
                NYTree::EErrorCode::ResolveError,
                "No such tablet cell bundle %Qv",
                name);
        }
        return cellBundle;
    }

    void RenameCellBundle(TCellBundle* cellBundle, const TString& newName)
    {
        if (newName == cellBundle->GetName()) {
            return;
        }

        ValidateCellBundleName(newName);

        if (FindCellBundleByName(newName)) {
            THROW_ERROR_EXCEPTION(
                NYTree::EErrorCode::AlreadyExists,
                "Tablet cell bundle %Qv already exists",
                newName);
        }

        YT_VERIFY(NameToCellBundleMap_.erase(cellBundle->GetName()) == 1);
        YT_VERIFY(NameToCellBundleMap_.insert(std::make_pair(newName, cellBundle)).second);
        cellBundle->SetName(newName);
    }

    void SetCellBundleNodeTagFilter(TCellBundle* bundle, const TString& formula)
    {
        if (bundle->NodeTagFilter().GetFormula() != formula) {
            bundle->NodeTagFilter() = MakeBooleanFormula(formula);
            CellBundleNodeTagFilterChanged_.Fire(bundle);
        }
    }

    DECLARE_ENTITY_MAP_ACCESSORS(CellBundle, TCellBundle);
    DECLARE_ENTITY_MAP_ACCESSORS(Cell, TCellBase);

private:
    template <class T>
    class TEntityMapTypeTraits
    {
    public:
        explicit TEntityMapTypeTraits(TBootstrap* bootstrap)
            : Bootstrap_(bootstrap)
        { }

        std::unique_ptr<T> Create(const TObjectId& id) const
        {
            auto type = TypeFromId(id);
            const auto& objectManager = Bootstrap_->GetObjectManager();
            const auto& handler = objectManager->FindHandler(type);
            auto objectHolder = handler->InstantiateObject(id);
            return std::unique_ptr<T>(objectHolder.release()->As<T>());
        }

    private:
        const TBootstrap* Bootstrap_;
    };

    const TCellTrackerPtr CellTracker_;
    const TBundleNodeTrackerPtr BundleNodeTracker_;

    TEntityMap<TCellBundle, TEntityMapTypeTraits<TCellBundle>> CellBundleMap_;
    TEntityMap<TCellBase, TEntityMapTypeTraits<TCellBase>> CellMap_;

    THashMap<TString, TCellBundle*> NameToCellBundleMap_;

    THashMap<TString, TCellSet> AddressToCell_;
    THashMap<TTransaction*, TCellBase*> TransactionToCellMap_;

    TPeriodicExecutorPtr CellStatusGossipExecutor_;

    DECLARE_THREAD_AFFINITY_SLOT(AutomatonThread);

    TPeriodicExecutorPtr ProfilingExecutor_;
    TProfiler Profiler = CellServerProfiler;


    const NTabletServer::TDynamicTabletManagerConfigPtr& GetDynamicConfig()
    {
        return Bootstrap_->GetConfigManager()->GetConfig()->TabletManager;
    }

    void OnDynamicConfigChanged()
    {
        const auto& config = GetDynamicConfig();

        if (CellStatusGossipExecutor_) {
            CellStatusGossipExecutor_->SetPeriod(config->MulticellGossip->TabletCellStatisticsGossipPeriod);
        }
    }

    void SaveKeys(NCellMaster::TSaveContext& context) const
    {
        CellBundleMap_.SaveKeys(context);
        CellMap_.SaveKeys(context);
    }

    void SaveValues(NCellMaster::TSaveContext& context) const
    {
        CellBundleMap_.SaveValues(context);
        CellMap_.SaveValues(context);
    }

    void LoadKeys(NCellMaster::TLoadContext& context)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        CellBundleMap_.LoadKeys(context);
        CellMap_.LoadKeys(context);
    }

    void LoadValues(NCellMaster::TLoadContext& context)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        CellBundleMap_.LoadValues(context);
        CellMap_.LoadValues(context);
    }


    virtual void OnAfterSnapshotLoaded() override
    {
        TMasterAutomatonPart::OnAfterSnapshotLoaded();

        // COMPAT(savrus)
        const auto& tabletManager = Bootstrap_->GetTabletManager();

        auto& bundleMap = tabletManager->CompatTabletCellBundleMap();
        std::vector<TCellBundleId> bundleIds;
        for (const auto [bundleId, holder] : bundleMap) {
            bundleIds.push_back(bundleId);
        }
        for (const auto bundleId : bundleIds) {
            CellBundleMap_.Insert(bundleId, bundleMap.Release(bundleId));
        }

        auto& cellMap = tabletManager->CompatTabletCellMap();
        std::vector<TCellBundleId> cellIds;
        for (const auto [cellId, holder] : cellMap) {
            cellIds.push_back(cellId);
        }
        for (const auto cellId : cellIds) {
            CellMap_.Insert(cellId, cellMap.Release(cellId));
        }

        NameToCellBundleMap_.clear();
        for (const auto [bundleId, bundle] : CellBundleMap_) {
            YT_VERIFY(NameToCellBundleMap_.insert(std::make_pair(bundle->GetName(), bundle)).second);
        }

        AddressToCell_.clear();
        for (const auto [cellId, cell] : CellMap_) {
            if (!IsObjectAlive(cell)) {
                continue;
            }

            YT_VERIFY(cell->GetCellBundle()->Cells().insert(cell).second);

            for (int peerId = 0; peerId < cell->Peers().size(); ++peerId) {
                const auto& peer = cell->Peers()[peerId];
                if (!peer.Descriptor.IsNull()) {
                    AddToAddressToCellMap(peer.Descriptor, cell, peerId);
                }
            }

            auto* transaction = cell->GetPrerequisiteTransaction();
            if (transaction) {
                YT_VERIFY(TransactionToCellMap_.insert(std::make_pair(transaction, cell)).second);
            }

            cell->GossipStatus().Initialize(Bootstrap_);
        }

        AfterSnapshotLoaded_.Fire();
    }

    virtual void Clear() override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        TMasterAutomatonPart::Clear();

        CellBundleMap_.Clear();
        CellMap_.Clear();
        NameToCellBundleMap_.clear();
        AddressToCell_.clear();
        TransactionToCellMap_.clear();

        BundleNodeTracker_->Clear();
    }

    void OnCellStatusGossip()
    {
        const auto& multicellManager = Bootstrap_->GetMulticellManager();
        if (!multicellManager->IsLocalMasterCellRegistered()) {
            return;
        }

        YT_LOG_INFO("Sending cell status gossip message");

        NProto::TReqSetCellStatus request;
        request.set_cell_tag(Bootstrap_->GetCellTag());

        for (const auto [cellId, cell] : CellMap_) {
            if (!IsObjectAlive(cell))
                continue;

            auto* entry = request.add_entries();
            ToProto(entry->mutable_cell_id(), cell->GetId());
            ToProto(
                entry->mutable_status(),
                multicellManager->IsPrimaryMaster() ? cell->GossipStatus().Cluster() : cell->GossipStatus().Local());
        }

        const auto& hydraManager = Bootstrap_->GetHydraFacade()->GetHydraManager();
        CreateMutation(hydraManager, TReqUpdateTabletCellHealthStatistics())
            ->CommitAndLog(Logger);

        if (multicellManager->IsPrimaryMaster()) {
            multicellManager->PostToSecondaryMasters(request, false);
        } else {
            multicellManager->PostToMaster(request, PrimaryMasterCellTag, false);
        }
    }

    void HydraSetCellStatus(NProto::TReqSetCellStatus* request)
    {
        const auto& multicellManager = Bootstrap_->GetMulticellManager();

        auto cellTag = request->cell_tag();
        YT_VERIFY(multicellManager->IsPrimaryMaster() || cellTag == Bootstrap_->GetPrimaryCellTag());

        if (!multicellManager->IsRegisteredMasterCell(cellTag)) {
            YT_LOG_ERROR_UNLESS(IsRecovery(), "Received cell status gossip message from unknown cell (CellTag: %v)",
                cellTag);
            return;
        }

        YT_LOG_INFO_UNLESS(IsRecovery(), "Received cell status gossip message (CellTag: %v)",
            cellTag);

        for (const auto& entry : request->entries()) {
            auto cellId = FromProto<TTamedCellId>(entry.cell_id());
            auto* cell = FindCell(cellId);
            if (!IsObjectAlive(cell))
                continue;

            auto newStatus = FromProto<TCellStatus>(entry.status());
            if (multicellManager->IsPrimaryMaster()) {
                *cell->GossipStatus().Remote(cellTag) = newStatus;
                cell->RecomputeClusterStatus();
            } else {
                cell->GossipStatus().Cluster() = newStatus;
            }
        }

        UpdateBundlesHealth();
    }

    void HydraUpdateCellHealth(TReqUpdateTabletCellHealthStatistics* request)
    {
        UpdateCellsHeath();
        UpdateBundlesHealth();
    }

    void UpdateCellsHeath()
    {
        for (const auto [cellId, cell] : CellMap_) {
            if (!IsObjectAlive(cell)) {
                continue;
            }

            auto& health = cell->GossipStatus().Local().Health;
            auto newHealth = cell->GetHealth();

            if (health != newHealth) {
                YT_LOG_DEBUG("Cell health changed (CellId: %v, OldHealth: %v, NewHealth: %v)",
                    cell->GetId(),
                    health,
                    newHealth);

                health = newHealth;
                const auto& multicellManager = Bootstrap_->GetMulticellManager();
                if (multicellManager->IsPrimaryMaster()) {
                    cell->RecomputeClusterStatus();
                }
            }
        }
    }

    void UpdateBundlesHealth()
    {
        for (const auto [bundleId, bundle] : CellBundleMap_) {
            if (!IsObjectAlive(bundle)) {
                continue;
            }

            auto oldHealth = bundle->Health();
            bundle->Health() = ECellHealth::Good;
            for (const auto& cell : bundle->Cells()) {
                bundle->Health() = TCellBase::CombineHealths(cell->GossipStatus().Local().Health, bundle->Health());
            }

            YT_LOG_DEBUG_IF(bundle->Health() != oldHealth,
                "Bundle health changed (Bundle: %v, OldHealth: %v, NewHealth: %v)",
                bundle->GetName(),
                oldHealth,
                bundle->Health());
        }
    }

    void UpdateNodeTabletSlotCount(TNode* node, int newSlotCount)
    {
        if (node->TabletSlots().size() == newSlotCount) {
            return;
        }

        YT_LOG_DEBUG("Node tablet slot count changed (Address: %v, OldTabletCellCount: %v, NewTabletCellCount: %v)",
            node->GetDefaultAddress(),
            node->TabletSlots().size(),
            newSlotCount);

        if (newSlotCount < node->TabletSlots().size()) {
            for (int slotIndex = newSlotCount; slotIndex < node->TabletSlots().size(); ++slotIndex) {
                const auto& slot = node->TabletSlots()[slotIndex];
                auto* cell = slot.Cell;
                if (cell) {
                    YT_LOG_DEBUG_UNLESS(IsRecovery(), "Tablet slot destroyed, detaching tablet cell peer (Address: %v, CellId: %v, PeerId: %v)",
                        node->GetDefaultAddress(),
                        cell->GetId(),
                        slot.PeerId);

                    cell->DetachPeer(node);
                }
            }
        }

        node->TabletSlots().resize(newSlotCount);
    }

    void OnNodeUnregistered(TNode* node)
    {
        YT_LOG_DEBUG_UNLESS(IsRecovery(), "Node unregistered (Address: %v)",
            node->GetDefaultAddress());

        UpdateNodeTabletSlotCount(node, 0);
    }

    void OnIncrementalHeartbeat(
        TNode* node,
        TReqIncrementalHeartbeat* request,
        TRspIncrementalHeartbeat* response)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        // Various request helpers.
        auto requestCreateSlot = [&] (const TCellBase* cell) {
            if (!response) {
                return;
            }

            const auto& multicellManager = Bootstrap_->GetMulticellManager();
            if (!multicellManager->IsPrimaryMaster() || !cell->GetPrerequisiteTransaction()) {
                return;
            }

            auto* protoInfo = response->add_tablet_slots_to_create();

            auto cellId = cell->GetId();
            auto peerId = cell->GetPeerId(node->GetDefaultAddress());

            ToProto(protoInfo->mutable_cell_id(), cell->GetId());
            protoInfo->set_peer_id(peerId);

            const auto* cellBundle = cell->GetCellBundle();
            protoInfo->set_options(ConvertToYsonString(cellBundle->GetOptions()).GetData());

            protoInfo->set_tablet_cell_bundle(cellBundle->GetName());

            YT_LOG_DEBUG_UNLESS(IsRecovery(), "Tablet slot creation requested (Address: %v, CellId: %v, PeerId: %v)",
                node->GetDefaultAddress(),
                cellId,
                peerId);
        };

        auto requestConfigureSlot = [&] (const TCellBase* cell) {
            if (!response) {
                return;
            }

            const auto& multicellManager = Bootstrap_->GetMulticellManager();
            if (!multicellManager->IsPrimaryMaster() || !cell->GetPrerequisiteTransaction()) {
                return;
            }

            auto* protoInfo = response->add_tablet_slots_configure();

            auto cellId = cell->GetId();
            auto peerId = cell->GetPeerId(node->GetDefaultAddress());

            auto cellDescriptor = cell->GetDescriptor();

            const auto& prerequisiteTransactionId = cell->GetPrerequisiteTransaction()->GetId();

            protoInfo->set_peer_id(peerId);
            ToProto(protoInfo->mutable_cell_descriptor(), cellDescriptor);
            ToProto(protoInfo->mutable_prerequisite_transaction_id(), prerequisiteTransactionId);
            protoInfo->set_abandon_leader_lease_during_recovery(GetDynamicConfig()->AbandonLeaderLeaseDuringRecovery);

            YT_LOG_DEBUG_UNLESS(IsRecovery(), "Tablet slot configuration update requested "
                "(Address: %v, CellId: %v, PeerId: %v, Version: %v, PrerequisiteTransactionId: %v, AbandonLeaderLeaseDuringRecovery: %v)",
                node->GetDefaultAddress(),
                cellId,
                peerId,
                cellDescriptor.ConfigVersion,
                prerequisiteTransactionId,
                protoInfo->abandon_leader_lease_during_recovery());
        };

        auto requestUpdateSlot = [&] (const TCellBase* cell) {
            if (!response) {
                return;
            }

            const auto& multicellManager = Bootstrap_->GetMulticellManager();
            if (!multicellManager->IsPrimaryMaster()) {
                return;
            }

            auto* protoInfo = response->add_tablet_slots_update();

            auto cellId = cell->GetId();

            ToProto(protoInfo->mutable_cell_id(), cell->GetId());

            const auto* cellBundle = cell->GetCellBundle();
            protoInfo->set_dynamic_config_version(cellBundle->GetDynamicConfigVersion());
            protoInfo->set_dynamic_options(ConvertToYsonString(cellBundle->GetDynamicOptions()).GetData());

            YT_LOG_DEBUG_UNLESS(IsRecovery(), "Tablet slot update requested (Address: %v, CellId: %v, DynamicConfigVersion: %v)",
                node->GetDefaultAddress(),
                cellId,
                cellBundle->GetDynamicConfigVersion());
        };

        auto requestRemoveSlot = [&] (TTamedCellId cellId) {
            if (!response) {
                return;
            }

            const auto& multicellManager = Bootstrap_->GetMulticellManager();
            if (!multicellManager->IsPrimaryMaster()) {
                return;
            }

            auto* protoInfo = response->add_tablet_slots_to_remove();
            ToProto(protoInfo->mutable_cell_id(), cellId);

            YT_LOG_DEBUG_UNLESS(IsRecovery(), "Tablet slot removal requested (Address: %v, CellId: %v)",
                node->GetDefaultAddress(),
                cellId);
        };

        const auto* mutationContext = GetCurrentMutationContext();
        auto mutationTimestamp = mutationContext->GetTimestamp();

        const auto& address = node->GetDefaultAddress();

        UpdateNodeTabletSlotCount(node, request->tablet_slots_size());

        // Our expectations.
        THashSet<TCellBase*> expectedCells;
        for (const auto& slot : node->TabletSlots()) {
            auto* cell = slot.Cell;
            if (!IsObjectAlive(cell)) {
                continue;
            }
            YT_VERIFY(expectedCells.insert(cell).second);
        }

        THashMap<TTamedCellId, EPeerState> cellIdToPeerState;

        // Figure out and analyze the reality.
        THashSet<const TCellBase*> actualCells;
        for (int slotIndex = 0; slotIndex < request->tablet_slots_size(); ++slotIndex) {
            // Pre-erase slot.
            auto& slot = node->TabletSlots()[slotIndex];
            slot = TNode::TCellSlot();

            const auto& slotInfo = request->tablet_slots(slotIndex);

            auto state = EPeerState(slotInfo.peer_state());
            if (state == EPeerState::None)
                continue;

            auto cellInfo = FromProto<TCellInfo>(slotInfo.cell_info());
            auto cellId = cellInfo.CellId;
            auto* cell = FindCell(cellId);
            if (!IsObjectAlive(cell)) {
                YT_LOG_DEBUG_UNLESS(IsRecovery(), "Unknown tablet slot is running (Address: %v, CellId: %v)",
                    address,
                    cellId);
                requestRemoveSlot(cellId);
                continue;
            }

            auto peerId = cell->FindPeerId(address);
            if (peerId == InvalidPeerId) {
                YT_LOG_DEBUG_UNLESS(IsRecovery(), "Unexpected tablet cell is running (Address: %v, CellId: %v)",
                    address,
                    cellId);
                requestRemoveSlot(cellId);
                continue;
            }

            if (CountVotingPeers(cell) > 1 && slotInfo.peer_id() != InvalidPeerId && slotInfo.peer_id() != peerId) {
                YT_LOG_DEBUG_UNLESS(
                    IsRecovery(),
                    "Invalid peer id for tablet cell: %v instead of %v (Address: %v, CellId: %v)",
                    slotInfo.peer_id(),
                    peerId,
                    address,
                    cellId);
                requestRemoveSlot(cellId);
                continue;
            }

            if (state == EPeerState::Stopped) {
                YT_LOG_DEBUG_UNLESS(IsRecovery(), "Peer is stopped, removing (PeerId: %v, Address: %v, CellId: %v)",
                    slotInfo.peer_id(),
                    address,
                    cellId);
                requestRemoveSlot(cellId);
                continue;
            }

            auto expectedIt = expectedCells.find(cell);
            if (expectedIt == expectedCells.end()) {
                cell->AttachPeer(node, peerId);
                YT_LOG_DEBUG_UNLESS(IsRecovery(), "Tablet cell peer online (Address: %v, CellId: %v, PeerId: %v)",
                    address,
                    cellId,
                    peerId);
            }

            cellIdToPeerState.emplace(cellId, state);

            cell->UpdatePeerSeenTime(peerId, mutationTimestamp);
            cell->UpdatePeerState(peerId, state);
            YT_VERIFY(actualCells.insert(cell).second);

            // Populate slot.
            slot.Cell = cell;
            slot.PeerState = state;
            slot.PeerId = slot.Cell->GetPeerId(node); // don't trust peerInfo, it may still be InvalidPeerId
            slot.IsResponseKeeperWarmingUp = slotInfo.is_response_keeper_warming_up();
            slot.PreloadPendingStoreCount = slotInfo.preload_pending_store_count();
            slot.PreloadCompletedStoreCount = slotInfo.preload_completed_store_count();
            slot.PreloadFailedStoreCount = slotInfo.preload_failed_store_count();

            YT_LOG_DEBUG_UNLESS(IsRecovery(), "Tablet cell is running (Address: %v, CellId: %v, PeerId: %v, State: %v, ConfigVersion: %v)",
                address,
                cell->GetId(),
                slot.PeerId,
                state,
                cellInfo.ConfigVersion);

            if (cellInfo.ConfigVersion != cell->GetConfigVersion()) {
                requestConfigureSlot(cell);
            }

            if (slotInfo.has_dynamic_config_version() &&
                slotInfo.dynamic_config_version() != cell->GetCellBundle()->GetDynamicConfigVersion())
            {
                requestUpdateSlot(cell);
            }
        }

        // Check for expected slots that are missing.
        for (auto* cell : expectedCells) {
            if (actualCells.find(cell) == actualCells.end()) {
                YT_LOG_DEBUG_UNLESS(IsRecovery(), "Tablet cell peer offline: slot is missing (CellId: %v, Address: %v)",
                    cell->GetId(),
                    address);
                cell->DetachPeer(node);
            }
        }

        // Request slot starts.
        {
            int availableSlots = node->Statistics().available_tablet_slots();
            auto it = AddressToCell_.find(address);
            if (it != AddressToCell_.end()) {
                for (auto [cell, peerId] : it->second) {
                    if (!IsObjectAlive(cell)) {
                        continue;
                    }
                    if (actualCells.find(cell) == actualCells.end()) {
                        requestCreateSlot(cell);
                        requestConfigureSlot(cell);
                        requestUpdateSlot(cell);
                        --availableSlots;
                    }
                }
            }
        }
    }


    void AddToAddressToCellMap(const TNodeDescriptor& descriptor, TCellBase* cell, int peerId)
    {
        const auto& address = descriptor.GetDefaultAddress();
        auto cellsIt = AddressToCell_.find(address);
        if (cellsIt == AddressToCell_.end()) {
            cellsIt = AddressToCell_.insert(std::make_pair(address, TCellSet())).first;
        }
        auto& set = cellsIt->second;
        auto it = std::find_if(set.begin(), set.end(), [&] (const auto& pair) {
            return pair.first == cell;
        });
        YT_VERIFY(it == set.end());
        set.emplace_back(cell, peerId);
    }

    void RemoveFromAddressToCellMap(const TNodeDescriptor& descriptor, TCellBase* cell)
    {
        const auto& address = descriptor.GetDefaultAddress();
        auto cellsIt = AddressToCell_.find(address);
        YT_VERIFY(cellsIt != AddressToCell_.end());
        auto& set = cellsIt->second;
        auto it = std::find_if(set.begin(), set.end(), [&] (const auto& pair) {
            return pair.first == cell;
        });
        YT_VERIFY(it != set.end());
        set.erase(it);
        if (set.empty()) {
            AddressToCell_.erase(cellsIt);
        }
    }

    void HydraAssignPeers(TReqAssignPeers* request)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        auto cellId = FromProto<TTamedCellId>(request->cell_id());
        auto* cell = FindCell(cellId);
        if (!IsObjectAlive(cell)) {
            return;
        }

        const auto* mutationContext = GetCurrentMutationContext();
        auto mutationTimestamp = mutationContext->GetTimestamp();

        bool leadingPeerAssigned = false;
        for (const auto& peerInfo : request->peer_infos()) {
            auto peerId = peerInfo.peer_id();
            auto descriptor = FromProto<TNodeDescriptor>(peerInfo.node_descriptor());

            auto& peer = cell->Peers()[peerId];
            if (!peer.Descriptor.IsNull()) {
                continue;
            }

            if (peerId == cell->GetLeadingPeerId()) {
                leadingPeerAssigned = true;
            }

            AddToAddressToCellMap(descriptor, cell, peerId);
            cell->AssignPeer(descriptor, peerId);
            cell->UpdatePeerSeenTime(peerId, mutationTimestamp);
            YT_LOG_DEBUG_UNLESS(IsRecovery(), "Tablet cell peer assigned (CellId: %v, Address: %v, PeerId: %v)",
                cellId,
                descriptor.GetDefaultAddress(),
                peerId);
        }

        const auto& multicellManager = Bootstrap_->GetMulticellManager();
        if (multicellManager->IsPrimaryMaster()) {
            // Once a peer is assigned, we must ensure that the cell has a valid prerequisite transaction.
            if (leadingPeerAssigned || !cell->GetPrerequisiteTransaction()) {
                RestartPrerequisiteTransaction(cell);
            }

            const auto& multicellManager = Bootstrap_->GetMulticellManager();
            multicellManager->PostToMasters(*request, multicellManager->GetRegisteredMasterCellTags());
        }

        ReconfigureCell(cell);
    }

    void HydraRevokePeers(TReqRevokePeers* request)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        auto cellId = FromProto<TTamedCellId>(request->cell_id());
        auto* cell = FindCell(cellId);
        if (!IsObjectAlive(cell)) {
            return;
        }

        bool leadingPeerRevoked = false;
        for (auto peerId : request->peer_ids()) {
            if (peerId == cell->GetLeadingPeerId()) {
                leadingPeerRevoked = true;
            }
            DoRevokePeer(cell, peerId);
        }

        const auto& multicellManager = Bootstrap_->GetMulticellManager();
        if (multicellManager->IsPrimaryMaster()) {
            if (leadingPeerRevoked) {
                AbortPrerequisiteTransaction(cell);
                AbortCellSubtreeTransactions(cell);
            }

            const auto& multicellManager = Bootstrap_->GetMulticellManager();
            multicellManager->PostToMasters(*request, multicellManager->GetRegisteredMasterCellTags());
        }

        ReconfigureCell(cell);
    }

    void HydraReassignPeers(TReqReassignPeers* request)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        for (auto& revocation : *request->mutable_revocations()) {
            HydraRevokePeers(&revocation);
        }

        for (auto& assignment : *request->mutable_assignments()) {
            HydraAssignPeers(&assignment);
        }

        for (auto& peer_count_update : *request->mutable_peer_count_updates()) {
            HydraUpdatePeerCount(&peer_count_update);
        }

        CellPeersAssigned_.Fire();

        // NB: Send individual revoke and assign requests to secondary masters to support old tablet tracker.
    }

    void HydraSetLeadingPeer(TReqSetLeadingPeer* request)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        auto cellId = FromProto<TTamedCellId>(request->cell_id());
        auto* cell = FindCell(cellId);
        const auto& oldLeader = cell->Peers()[cell->GetLeadingPeerId()];
        if (!IsObjectAlive(cell)) {
            return;
        }

        auto peerId = request->peer_id();
        const auto& newLeader = cell->Peers()[peerId];

        cell->SetLeadingPeerId(peerId);

        const auto& descriptor = cell->Peers()[peerId].Descriptor;
        YT_LOG_DEBUG_UNLESS(IsRecovery(), "Tablet cell leading peer updated (CellId: %v, Address: %v, PeerId: %v)",
            cellId,
            descriptor.GetDefaultAddress(),
            peerId);

        const auto& multicellManager = Bootstrap_->GetMulticellManager();
        if (multicellManager->IsPrimaryMaster()) {
            RestartPrerequisiteTransaction(cell);

            multicellManager->PostToMasters(*request, multicellManager->GetRegisteredMasterCellTags());
        }

        ReconfigureCell(cell);

        // Notify new leader as soon as possible via heartbeat request.
        if (multicellManager->IsPrimaryMaster() && IsLeader()) {
            if (oldLeader.Node) {
                Bootstrap_->GetNodeTracker()->RequestNodeHeartbeat(oldLeader.Node->GetId());
            }
            if (newLeader.Node) {
                Bootstrap_->GetNodeTracker()->RequestNodeHeartbeat(newLeader.Node->GetId());
            }
        }
    }

    virtual void OnLeaderActive() override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        TMasterAutomatonPart::OnLeaderActive();

        const auto& multicellManager = Bootstrap_->GetMulticellManager();
        if (multicellManager->IsPrimaryMaster()) {
            CellTracker_->Start();
        }

        CellStatusGossipExecutor_ = New<TPeriodicExecutor>(
            Bootstrap_->GetHydraFacade()->GetEpochAutomatonInvoker(NCellMaster::EAutomatonThreadQueue::TabletGossip),
            BIND(&TImpl::OnCellStatusGossip, MakeWeak(this)));
        CellStatusGossipExecutor_->Start();

        OnDynamicConfigChanged();
    }

    virtual void OnStopLeading() override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        TMasterAutomatonPart::OnStopLeading();

        CellTracker_->Stop();

        if (CellStatusGossipExecutor_) {
            CellStatusGossipExecutor_->Stop();
            CellStatusGossipExecutor_.Reset();
        }
    }

    void ReconfigureCell(TCellBase* cell)
    {
        cell->SetConfigVersion(cell->GetConfigVersion() + 1);

        auto config = cell->GetConfig();
        config->Addresses.clear();
        for (const auto& peer : cell->Peers()) {
            if (peer.Descriptor.IsNull()) {
                config->Addresses.push_back(std::nullopt);
            } else {
                config->Addresses.push_back(peer.Descriptor.GetAddressOrThrow(Bootstrap_->GetConfig()->Networks));
            }
        }

        YT_LOG_DEBUG_UNLESS(
            IsRecovery(),
            "Tablet cell reconfigured (CellId: %v, Version: %v)",
            cell->GetId(),
            cell->GetConfigVersion());
    }

    bool CheckHasHealthyCells(TCellBundle* bundle)
    {
        for (const auto [cellId, cell] : CellMap_) {
            if (!IsCellActive(cell)) {
                continue;
            }
            if (cell->GetCellBundle() == bundle &&
                cell->GetHealth() == ECellHealth::Good)
            {
                return true;
            }
        }

        return false;
    }

    void ValidateHasHealthyCells(TCellBundle* bundle)
    {
        if (!CheckHasHealthyCells(bundle)) {
            THROW_ERROR_EXCEPTION("No healthy tablet cells in bundle %Qv",
                bundle->GetName());
        }
    }

    bool IsCellActive(TCellBase* cell)
    {
        return IsObjectAlive(cell) && !cell->IsDecommissionStarted();
    }

    void RestartPrerequisiteTransaction(TCellBase* cell)
    {
        const auto& multicellManager = Bootstrap_->GetMulticellManager();
        YT_VERIFY(multicellManager->IsPrimaryMaster());

        AbortPrerequisiteTransaction(cell);
        AbortCellSubtreeTransactions(cell);
        StartPrerequisiteTransaction(cell);
    }

    void StartPrerequisiteTransaction(TCellBase* cell)
    {
        const auto& multicellManager = Bootstrap_->GetMulticellManager();
        YT_VERIFY(multicellManager->IsPrimaryMaster());

        const auto& secondaryCellTags = multicellManager->GetRegisteredMasterCellTags();

        const auto& transactionManager = Bootstrap_->GetTransactionManager();
        auto* transaction = transactionManager->StartTransaction(
            nullptr,
            {},
            secondaryCellTags,
            /* replicateStart */ true,
            std::nullopt,
            /* deadline */ std::nullopt,
            Format("Prerequisite for cell %v", cell->GetId()),
            EmptyAttributes());

        YT_VERIFY(!cell->GetPrerequisiteTransaction());
        cell->SetPrerequisiteTransaction(transaction);
        YT_VERIFY(TransactionToCellMap_.insert(std::make_pair(transaction, cell)).second);

        TReqStartPrerequisiteTransaction request;
        ToProto(request.mutable_cell_id(), cell->GetId());
        ToProto(request.mutable_transaction_id(), transaction->GetId());
        multicellManager->PostToMasters(request, multicellManager->GetRegisteredMasterCellTags());

        YT_LOG_DEBUG_UNLESS(IsRecovery(), "Tablet cell prerequisite transaction started (CellId: %v, TransactionId: %v)",
            cell->GetId(),
            transaction->GetId());
    }

    void HydraStartPrerequisiteTransaction(TReqStartPrerequisiteTransaction* request)
    {
        YT_VERIFY(Bootstrap_->IsSecondaryMaster());

        auto cellId = FromProto<TTamedCellId>(request->cell_id());
        auto transactionId = FromProto<TTransactionId>(request->transaction_id());

        auto* cell = FindCell(cellId);
        if (!IsObjectAlive(cell)) {
            return;
        }

        const auto& transactionManager = Bootstrap_->GetTransactionManager();
        auto transaction = transactionManager->FindTransaction(transactionId);

        if (!IsObjectAlive(transaction)) {
            YT_LOG_INFO("Prerequisite transaction is not found on secondary master (CellId: %v, TransactionId: %v)",
                cellId,
                transactionId);
            return;
        }

        YT_VERIFY(TransactionToCellMap_.insert(std::make_pair(transaction, cell)).second);

        YT_LOG_DEBUG_UNLESS(IsRecovery(), "Tablet cell prerequisite transaction attached (CellId: %v, TransactionId: %v)",
            cell->GetId(),
            transaction->GetId());
    }

    void AbortCellSubtreeTransactions(TCellBase* cell)
    {
        const auto& cypressManager = Bootstrap_->GetCypressManager();
        auto cellNodeProxy = FindCellNode(cell->GetId());
        if (cellNodeProxy) {
            cypressManager->AbortSubtreeTransactions(cellNodeProxy);
        }
    }

    void AbortPrerequisiteTransaction(TCellBase* cell)
    {
        const auto& multicellManager = Bootstrap_->GetMulticellManager();
        YT_VERIFY(multicellManager->IsPrimaryMaster());

        auto* transaction = cell->GetPrerequisiteTransaction();
        if (!transaction) {
            return;
        }

        // Suppress calling OnTransactionFinished.
        YT_VERIFY(TransactionToCellMap_.erase(transaction) == 1);
        cell->SetPrerequisiteTransaction(nullptr);

        // Suppress calling OnTransactionFinished on secondary masters.
        TReqAbortPrerequisiteTransaction request;
        ToProto(request.mutable_cell_id(), cell->GetId());
        ToProto(request.mutable_transaction_id(), transaction->GetId());
        multicellManager->PostToMasters(request, multicellManager->GetRegisteredMasterCellTags());

        // NB: Make a copy, transaction will die soon.
        auto transactionId = transaction->GetId();

        const auto& transactionManager = Bootstrap_->GetTransactionManager();
        transactionManager->AbortTransaction(transaction, true);

        YT_LOG_DEBUG_UNLESS(IsRecovery(), "Tablet cell prerequisite transaction aborted (CellId: %v, TransactionId: %v)",
            cell->GetId(),
            transactionId);
    }

    void HydraAbortPrerequisiteTransaction(TReqAbortPrerequisiteTransaction* request)
    {
        YT_VERIFY(Bootstrap_->IsSecondaryMaster());

        auto cellId = FromProto<TTamedCellId>(request->cell_id());
        auto transactionId = FromProto<TTransactionId>(request->transaction_id());

        const auto& transactionManager = Bootstrap_->GetTransactionManager();
        auto transaction = transactionManager->FindTransaction(transactionId);

        if (!IsObjectAlive(transaction)) {
            YT_LOG_WARNING("Unexpected error: prerequisite transaction not found at secondary master (CellId: %v, TransactionId: %v)",
                cellId,
                transactionId);
            return;
        }

        // COMPAT(savrus) Don't check since we didn't have them in earlier versions.
        TransactionToCellMap_.erase(transaction);

        YT_LOG_DEBUG_UNLESS(IsRecovery(), "Tablet cell prerequisite transaction aborted (CellId: %v, TransactionId: %v)",
            cellId,
            transactionId);
    }

    void OnTransactionFinished(TTransaction* transaction)
    {
        auto it = TransactionToCellMap_.find(transaction);
        if (it == TransactionToCellMap_.end()) {
            return;
        }

        auto* cell = it->second;
        cell->SetPrerequisiteTransaction(nullptr);
        TransactionToCellMap_.erase(it);

        YT_LOG_DEBUG_UNLESS(IsRecovery(), "Tablet cell prerequisite transaction finished (CellId: %v, TransactionId: %v)",
            cell->GetId(),
            transaction->GetId());

        for (auto peerId = 0; peerId < cell->Peers().size(); ++peerId) {
            DoRevokePeer(cell, peerId);
        }
    }

    void DoRevokePeer(TCellBase* cell, TPeerId peerId)
    {
        const auto& peer = cell->Peers()[peerId];
        const auto& descriptor = peer.Descriptor;
        if (descriptor.IsNull()) {
            return;
        }

        YT_LOG_DEBUG_UNLESS(IsRecovery(), "Tablet cell peer revoked (CellId: %v, Address: %v, PeerId: %v)",
            cell->GetId(),
            descriptor.GetDefaultAddress(),
            peerId);

        if (peer.Node) {
            peer.Node->DetachTabletCell(cell);
        }
        RemoveFromAddressToCellMap(descriptor, cell);
        cell->RevokePeer(peerId);
    }

    IMapNodePtr GetCellMapNode()
    {
        const auto& cypressManager = Bootstrap_->GetCypressManager();
        return cypressManager->ResolvePathToNodeProxy("//sys/tablet_cells")->AsMap();
    }

    INodePtr FindCellNode(TTamedCellId cellId)
    {
        auto cellMapNodeProxy = GetCellMapNode();
        return cellMapNodeProxy->FindChild(ToString(cellId));
    }


    void OnReplicateKeysToSecondaryMaster(TCellTag cellTag)
    {
        const auto& objectManager = Bootstrap_->GetObjectManager();

        auto cellBundles = GetValuesSortedByKey(CellBundleMap_);
        for (auto* cellBundle : cellBundles) {
            objectManager->ReplicateObjectCreationToSecondaryMaster(cellBundle, cellTag);
        }

        auto cells = GetValuesSortedByKey(CellMap_);
        for (auto* cell : cells) {
            objectManager->ReplicateObjectCreationToSecondaryMaster(cell, cellTag);
        }
    }

    void OnReplicateValuesToSecondaryMaster(TCellTag cellTag)
    {
        const auto& objectManager = Bootstrap_->GetObjectManager();

        auto cellBundles = GetValuesSortedByKey(CellBundleMap_);
        for (auto* cellBundle : cellBundles) {
            objectManager->ReplicateObjectAttributesToSecondaryMaster(cellBundle, cellTag);
        }

        auto cells = GetValuesSortedByKey(CellMap_);
        for (auto* cell : cells) {
            objectManager->ReplicateObjectAttributesToSecondaryMaster(cell, cellTag);
            ReplicateCellPropertiesToSecondaryMaster(cell, cellTag);
        }
    }

    void ReplicateCellPropertiesToSecondaryMaster(TCellBase* cell, TCellTag cellTag)
    {
        const auto& multicellManager = Bootstrap_->GetMulticellManager();

        {
            NTabletServer::NProto::TReqSetTabletCellConfigVersion req;
            ToProto(req.mutable_cell_id(), cell->GetId());
            req.set_config_version(cell->GetConfigVersion());
            multicellManager->PostToMaster(req, cellTag);
        }

        if (cell->IsDecommissionStarted()) {
            NTabletServer::NProto::TReqDecommissionTabletCellOnMaster req;
            ToProto(req.mutable_cell_id(), cell->GetId());
            multicellManager->PostToMaster(req, cellTag);
        }
    }

    void HydraSetCellConfigVersion(NTabletServer::NProto::TReqSetTabletCellConfigVersion* request)
    {
        auto cellId = FromProto<TTamedCellId>(request->cell_id());
        auto* cell = FindCell(cellId);
        if (!IsObjectAlive(cell))
            return;
        cell->SetConfigVersion(request->config_version());
    }

    void OnProfiling()
    {
        if (!IsLeader()) {
            return;
        }

        const auto& multicellManager = Bootstrap_->GetMulticellManager();
        if (!multicellManager->IsPrimaryMaster()) {
            return;
        }

        for (const auto& [id, cellBundle] : CellBundleMap_) {
            auto tagIds = TTagIdList{cellBundle->GetProfilingTag()};
            Profiler.Enqueue("/tablet_cell_count", cellBundle->Cells().size(), EMetricType::Gauge, tagIds);
        }
    }

    static void ValidateCellBundleName(const TString& name)
    {
        if (name.empty()) {
            THROW_ERROR_EXCEPTION("Tablet cell bundle name cannot be empty");
        }
    }

    static int CountVotingPeers(const TCellBase* cell)
    {
        int votingPeers = 0;
        for (const auto& peer : cell->GetDescriptor().Peers) {
            if (peer.GetVoting()) {
                ++votingPeers;
            }
        }

        return votingPeers;
    }
};

DEFINE_ENTITY_MAP_ACCESSORS(TTamedCellManager::TImpl, CellBundle, TCellBundle, CellBundleMap_)
DEFINE_ENTITY_MAP_ACCESSORS(TTamedCellManager::TImpl, Cell, TCellBase, CellMap_)

////////////////////////////////////////////////////////////////////////////////

TTamedCellManager::TTamedCellManager(NCellMaster::TBootstrap* bootstrap)
    : Impl_(New<TImpl>(bootstrap))
{ }

TTamedCellManager::~TTamedCellManager() = default;

void TTamedCellManager::Initialize()
{
    return Impl_->Initialize();
}

const TCellSet* TTamedCellManager::FindAssignedCells(const TString& address) const
{
    return Impl_->FindAssignedCells(address);
}

const TBundleNodeTrackerPtr& TTamedCellManager::GetBundleNodeTracker()
{
    return Impl_->GetBundleNodeTracker();
}

TCellBase* TTamedCellManager::GetCellOrThrow(TTamedCellId id)
{
    return Impl_->GetCellOrThrow(id);
}

void TTamedCellManager::RemoveCell(TCellBase* cell, bool force)
{
    return Impl_->RemoveCell(cell, force);
}

TCellBundle* TTamedCellManager::FindCellBundleByName(const TString& name)
{
    return Impl_->FindCellBundleByName(name);
}

TCellBundle* TTamedCellManager::GetCellBundleByNameOrThrow(const TString& name)
{
    return Impl_->GetCellBundleByNameOrThrow(name);
}

void TTamedCellManager::RenameCellBundle(TCellBundle* cellBundle, const TString& newName)
{
    return Impl_->RenameCellBundle(cellBundle, newName);
}

void TTamedCellManager::SetCellBundleNodeTagFilter(TCellBundle* bundle, const TString& formula)
{
    return Impl_->SetCellBundleNodeTagFilter(bundle, formula);
}

TCellBase* TTamedCellManager::CreateCell(TCellBundle* cellBundle, std::unique_ptr<TCellBase> holder)
{
    return Impl_->CreateCell(cellBundle, std::move(holder));
}

void TTamedCellManager::ZombifyCell(TCellBase* cell)
{
    Impl_->ZombifyCell(cell);
}

void TTamedCellManager::DestroyCell(TCellBase* cell)
{
    Impl_->DestroyCell(cell);
}

void TTamedCellManager::UpdatePeerCount(TCellBase* cell, std::optional<int> peerCount)
{
    Impl_->UpdatePeerCount(cell, peerCount);
}

TCellBundle* TTamedCellManager::CreateCellBundle(
    const TString& name,
    std::unique_ptr<TCellBundle> holder,
    TTabletCellOptionsPtr options)
{
    return Impl_->CreateCellBundle(name, std::move(holder), std::move(options));
}

void TTamedCellManager::ZombifyCellBundle(TCellBundle* cellBundle)
{
    Impl_->ZombifyCellBundle(cellBundle);
}

void TTamedCellManager::DestroyCellBundle(TCellBundle* cellBundle)
{
    Impl_->DestroyCellBundle(cellBundle);
}

void TTamedCellManager::SetCellBundleOptions(TCellBundle* cellBundle, TTabletCellOptionsPtr options)
{
    Impl_->SetCellBundleOptions(cellBundle, std::move(options));
}

DELEGATE_ENTITY_MAP_ACCESSORS(TTamedCellManager, CellBundle, TCellBundle, *Impl_)
DELEGATE_ENTITY_MAP_ACCESSORS(TTamedCellManager, Cell, TCellBase, *Impl_)

DELEGATE_SIGNAL(TTamedCellManager, void(TCellBundle*), CellBundleCreated, *Impl_);
DELEGATE_SIGNAL(TTamedCellManager, void(TCellBundle*), CellBundleDestroyed, *Impl_);
DELEGATE_SIGNAL(TTamedCellManager, void(TCellBundle*), CellBundleNodeTagFilterChanged, *Impl_);
DELEGATE_SIGNAL(TTamedCellManager, void(TCellBase*), CellDecommissionStarted, *Impl_);
DELEGATE_SIGNAL(TTamedCellManager, void(), CellPeersAssigned, *Impl_);
DELEGATE_SIGNAL(TTamedCellManager, void(), AfterSnapshotLoaded, *Impl_);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellServer
