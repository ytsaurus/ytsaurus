#include "world_initializer.h"
#include "private.h"
#include "config.h"
#include "hydra_facade.h"

#include <yt/server/cell_master/bootstrap.h>

#include <yt/server/cypress_server/cypress_manager.h>
#include <yt/server/cypress_server/node_detail.h>

#include <yt/server/hive/transaction_supervisor.h>

#include <yt/server/security_server/acl.h>
#include <yt/server/security_server/group.h>
#include <yt/server/security_server/security_manager.h>

#include <yt/ytlib/cypress_client/cypress_ypath_proxy.h>
#include <yt/ytlib/cypress_client/rpc_helpers.h>

#include <yt/ytlib/election/cell_manager.h>

#include <yt/ytlib/object_client/helpers.h>
#include <yt/ytlib/object_client/master_ypath_proxy.h>
#include <yt/ytlib/object_client/object_service_proxy.h>

#include <yt/ytlib/transaction_client/transaction_ypath.pb.h>

#include <yt/core/concurrency/scheduler.h>

#include <yt/core/logging/log.h>

#include <yt/core/misc/collection_helpers.h>

#include <yt/core/ypath/token.h>

#include <yt/core/ytree/ypath_client.h>
#include <yt/core/ytree/ypath_proxy.h>

#include <yt/server/transaction_server/transaction_manager.h>

namespace NYT {
namespace NCellMaster {

using namespace NConcurrency;
using namespace NYTree;
using namespace NYson;
using namespace NYPath;
using namespace NCypressServer;
using namespace NCypressClient;
using namespace NTransactionClient;
using namespace NTransactionClient::NProto;
using namespace NHive;
using namespace NHive::NProto;
using namespace NObjectClient;
using namespace NObjectServer;
using namespace NSecurityServer;

using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = CellMasterLogger;
static const auto InitRetryPeriod = TDuration::Seconds(3);
static const auto InitTransactionTimeout = TDuration::Seconds(60);

////////////////////////////////////////////////////////////////////////////////

class TWorldInitializer::TImpl
    : public TRefCounted
{
public:
    TImpl(
        TCellMasterConfigPtr config,
        TBootstrap* bootstrap)
        : Config_(config)
        , Bootstrap_(bootstrap)
    {
        YCHECK(Config_);
        YCHECK(Bootstrap_);

        auto hydraManager = Bootstrap_->GetHydraFacade()->GetHydraManager();
        hydraManager->SubscribeLeaderActive(BIND(&TImpl::OnLeaderActive, MakeWeak(this)));
    }


    bool IsInitialized()
    {
        auto cypressManager = Bootstrap_->GetCypressManager();
        auto* root = dynamic_cast<TMapNode*>(cypressManager->GetRootNode());
        YCHECK(root);
        return !root->KeyToChild().empty();
    }

    void ValidateInitialized()
    {
        if (!IsInitialized()) {
            THROW_ERROR_EXCEPTION(NRpc::EErrorCode::Unavailable, "Cluster is not initialized");
        }
    }

    bool HasProvisionLock()
    {
        auto cypressManager = Bootstrap_->GetCypressManager();
        auto resolver = cypressManager->CreateResolver();
        auto sysNode = resolver->ResolvePath("//sys");
        return sysNode->Attributes().Get<bool>("provision_lock", false);
    }

private:
    const TCellMasterConfigPtr Config_;
    TBootstrap* const Bootstrap_;

    std::vector<TFuture<void>> ScheduledMutations_;


    void OnLeaderActive()
    {
        // NB: Initialization cannot be carried out here since not all subsystems
        // are fully initialized yet.
        // We'll post an initialization callback to the automaton invoker instead.
        ScheduleInitialize();
    }

    void InitializeIfNeeded()
    {
        if (IsInitialized()) {
            LOG_INFO("World is already initialized");
        } else {
            Initialize();
        }
    }

    void ScheduleInitialize(TDuration delay = TDuration::Zero())
    {
        TDelayedExecutor::Submit(
            BIND(&TImpl::InitializeIfNeeded, MakeStrong(this))
                .Via(Bootstrap_->GetHydraFacade()->GetEpochAutomatonInvoker()),
            delay);
    }

    void Initialize()
    {
        LOG_INFO("World initialization started");

        try {
            // Check for pre-existing transactions to avoid collisions with previous (failed)
            // initialization attempts.
            auto transactionManager = Bootstrap_->GetTransactionManager();
            if (transactionManager->Transactions().GetSize() > 0) {
                AbortTransactions();
                THROW_ERROR_EXCEPTION("World initialization aborted: transactions found");
            }

            auto objectManager = Bootstrap_->GetObjectManager();
            auto cypressManager = Bootstrap_->GetCypressManager();
            auto securityManager = Bootstrap_->GetSecurityManager();

            // All initialization will be happening within this transaction.
            auto transactionId = StartTransaction();

            // Level 1
            ScheduleCreateNode(
                "//sys",
                transactionId,
                EObjectType::SysNode,
                BuildYsonStringFluently()
                    .BeginMap()
                        .DoIf(Config_->EnableProvisionLock, [&] (TFluentMap fluent) {
                            fluent.Item("provision_lock").Value(true);
                        })
                    .EndMap());

            ScheduleCreateNode(
                "//tmp",
                transactionId,
                EObjectType::MapNode,
                BuildYsonStringFluently()
                    .BeginMap()
                        .Item("opaque").Value(true)
                        .Item("account").Value("tmp")
                        .Item("acl").BeginList()
                            .Item().Value(TAccessControlEntry(
                                ESecurityAction::Allow,
                                securityManager->GetUsersGroup(),
                                EPermissionSet(EPermission::Read | EPermission::Write | EPermission::Remove)))
                        .EndList()
                    .EndMap());

            FlushScheduled();

            // Level 2
            ScheduleCreateNode(
                "//sys/schemas",
                transactionId,
                EObjectType::MapNode,
                BuildYsonStringFluently()
                    .BeginMap()
                        .Item("opaque").Value(true)
                    .EndMap());

            ScheduleCreateNode(
                "//sys/scheduler",
                transactionId,
                EObjectType::MapNode,
                BuildYsonStringFluently()
                    .BeginMap()
                        .Item("opaque").Value(true)
                    .EndMap());

            ScheduleCreateNode(
                "//sys/pools",
                transactionId,
                EObjectType::MapNode,
                BuildYsonStringFluently()
                    .BeginMap()
                        .Item("opaque").Value(true)
                    .EndMap());

            ScheduleCreateNode(
                "//sys/tokens",
                transactionId,
                EObjectType::Document,
                BuildYsonStringFluently()
                    .BeginMap()
                        .Item("value").BeginMap()
                        .EndMap()
                    .EndMap());

            ScheduleCreateNode(
                "//sys/clusters",
                transactionId,
                EObjectType::Document,
                BuildYsonStringFluently()
                    .BeginMap()
                        .Item("value").BeginMap()
                        .EndMap()
                    .EndMap());

            ScheduleCreateNode(
                "//sys/empty_yamr_table",
                transactionId,
                EObjectType::Table,
                BuildYsonStringFluently()
                    .BeginMap()
                        .Item("external").Value(false)
                        .Item("schema")
                            .BeginAttributes()
                                .Item("strict").Value(true)
                            .EndAttributes()
                            .BeginList()
                                .Item()
                                    .BeginMap()
                                        .Item("name").Value("key")
                                        .Item("type").Value("string")
                                        .Item("sort_order").Value("ascending")
                                    .EndMap()
                                .Item()
                                    .BeginMap()
                                        .Item("name").Value("subkey")
                                        .Item("type").Value("string")
                                        .Item("sort_order").Value("ascending")
                                    .EndMap()
                                .Item()
                                    .BeginMap()
                                        .Item("name").Value("value")
                                        .Item("type").Value("string")
                                    .EndMap() 
                            .EndList()
                    .EndMap());

            ScheduleCreateNode(
                "//sys/scheduler/instances",
                transactionId,
                EObjectType::MapNode,
                BuildYsonStringFluently()
                    .BeginMap()
                        .Item("opaque").Value(true)
                    .EndMap());

            ScheduleCreateNode(
                "//sys/scheduler/orchid",
                transactionId,
                EObjectType::Orchid);

            ScheduleCreateNode(
                "//sys/scheduler/event_log",
                transactionId,
                EObjectType::Table,
                BuildYsonStringFluently()
                    .BeginMap()
                        .Item("external").Value(false)
                    .EndMap());

            ScheduleCreateNode(
                "//sys/operations",
                transactionId,
                EObjectType::MapNode,
                BuildYsonStringFluently()
                    .BeginMap()
                        .Item("opaque").Value(true)
                    .EndMap());

            ScheduleCreateNode(
                "//sys/proxies",
                transactionId,
                EObjectType::MapNode,
                BuildYsonStringFluently()
                    .BeginMap()
                        .Item("opaque").Value(true)
                    .EndMap());

            ScheduleCreateNode(
                "//sys/nodes",
                transactionId,
                EObjectType::ClusterNodeMap,
                BuildYsonStringFluently()
                    .BeginMap()
                        .Item("opaque").Value(true)
                    .EndMap());

            ScheduleCreateNode(
                "//sys/racks",
                transactionId,
                EObjectType::RackMap);

            ScheduleCreateNode(
                "//sys/primary_masters",
                transactionId,
                EObjectType::MapNode,
                BuildYsonStringFluently()
                    .BeginMap()
                        .Item("opaque").Value(true)
                    .EndMap());

            ScheduleCreateNode(
                "//sys/secondary_masters",
                transactionId,
                EObjectType::MapNode,
                BuildYsonStringFluently()
                    .BeginMap()
                        .Item("opaque").Value(true)
                    .EndMap());

            ScheduleCreateNode(
                "//sys/locks",
                transactionId,
                EObjectType::LockMap);

            ScheduleCreateNode(
                "//sys/chunks",
                transactionId,
                EObjectType::ChunkMap);

            ScheduleCreateNode(
                "//sys/lost_chunks",
                transactionId,
                EObjectType::LostChunkMap);

            ScheduleCreateNode(
                "//sys/lost_vital_chunks",
                transactionId,
                EObjectType::LostVitalChunkMap);

            ScheduleCreateNode(
                "//sys/overreplicated_chunks",
                transactionId,
                EObjectType::OverreplicatedChunkMap);

            ScheduleCreateNode(
                "//sys/underreplicated_chunks",
                transactionId,
                EObjectType::UnderreplicatedChunkMap);

            ScheduleCreateNode(
                "//sys/data_missing_chunks",
                transactionId,
                EObjectType::DataMissingChunkMap);

            ScheduleCreateNode(
                "//sys/parity_missing_chunks",
                transactionId,
                EObjectType::ParityMissingChunkMap);

            ScheduleCreateNode(
                "//sys/quorum_missing_chunks",
                transactionId,
                EObjectType::QuorumMissingChunkMap);

            ScheduleCreateNode(
                "//sys/unsafely_placed_chunks",
                transactionId,
                EObjectType::UnsafelyPlacedChunkMap);

            ScheduleCreateNode(
                "//sys/foreign_chunks",
                transactionId,
                EObjectType::ForeignChunkMap);

            ScheduleCreateNode(
                "//sys/chunk_lists",
                transactionId,
                EObjectType::ChunkListMap);

            ScheduleCreateNode(
                "//sys/transactions",
                transactionId,
                EObjectType::TransactionMap);

            ScheduleCreateNode(
                "//sys/topmost_transactions",
                transactionId,
                EObjectType::TopmostTransactionMap);

            ScheduleCreateNode(
                "//sys/accounts",
                transactionId,
                EObjectType::AccountMap);

            ScheduleCreateNode(
                "//sys/users",
                transactionId,
                EObjectType::UserMap);

            ScheduleCreateNode(
                "//sys/groups",
                transactionId,
                EObjectType::GroupMap);

            ScheduleCreateNode(
                "//sys/tablet_cell_bundles",
                transactionId,
                EObjectType::TabletCellBundleMap);

            ScheduleCreateNode(
                "//sys/tablet_cells",
                transactionId,
                EObjectType::MapNode,
                BuildYsonStringFluently()
                    .BeginMap()
                        .Item("opaque").Value(true)
                    .EndMap());

            ScheduleCreateNode(
                "//sys/tablets",
                transactionId,
                EObjectType::TabletMap);

            FlushScheduled();

            // Level 3

            for (auto type : objectManager->GetRegisteredTypes()) {
                if (HasSchema(type)) {
                    ScheduleCreateNode(
                        "//sys/schemas/" + ToYPathLiteral(FormatEnum(type)),
                        transactionId,
                        EObjectType::Link,
                        BuildYsonStringFluently()
                            .BeginMap()
                                .Item("target_id").Value(objectManager->GetSchema(type)->GetId())
                            .EndMap());
                }
            }

            ScheduleCreateNode(
                "//sys/scheduler/lock",
                transactionId,
                EObjectType::MapNode);

            auto createMasters = [&] (const TYPath& rootPath, NElection::TCellConfigPtr cellConfig) {
                for (const auto& peer : cellConfig->Peers) {
                    const auto& address = *peer.Address;
                    auto addressPath = "/" + ToYPathLiteral(address);
                    ScheduleCreateNode(
                        rootPath + addressPath + "/orchid",
                        transactionId,
                        EObjectType::Orchid,
                        BuildYsonStringFluently()
                            .BeginMap()
                                .Item("remote_address").Value(address)
                            .EndMap());
                }
            };

            createMasters("//sys/primary_masters", Config_->PrimaryMaster);

            for (auto cellConfig : Config_->SecondaryMasters) {
                auto cellTag = CellTagFromId(cellConfig->CellId);
                auto cellPath = "//sys/secondary_masters/" + ToYPathLiteral(cellTag);
                createMasters(cellPath, cellConfig);
            }

            FlushScheduled();

            CommitTransaction(transactionId);

            LOG_INFO("World initialization completed");
        } catch (const std::exception& ex) {
            LOG_ERROR(ex, "World initialization failed");
            AbandonScheduled();
            ScheduleInitialize(InitRetryPeriod);
        }
    }

    void AbortTransactions()
    {
        auto transactionManager = Bootstrap_->GetTransactionManager();
        auto transactionIds = ToObjectIds(GetValues(transactionManager->Transactions()));
        auto transactionSupervisor = Bootstrap_->GetTransactionSupervisor();
        for (const auto& transactionId : transactionIds) {
            transactionSupervisor->AbortTransaction(transactionId);
        }
    }

    TTransactionId StartTransaction()
    {
        auto service = Bootstrap_->GetObjectManager()->GetRootService();
        auto req = TMasterYPathProxy::CreateObject();
        req->set_type(static_cast<int>(EObjectType::Transaction));

        auto* requestExt = req->mutable_extensions()->MutableExtension(TTransactionCreationExt::transaction_creation_ext);
        requestExt->set_timeout(ToProto(InitTransactionTimeout));

        auto attributes = CreateEphemeralAttributes();
        attributes->Set("title", "World initialization");
        ToProto(req->mutable_object_attributes(), *attributes);

        auto rsp = WaitFor(ExecuteVerb(service, req))
            .ValueOrThrow();
        return FromProto<TTransactionId>(rsp->object_id());
    }

    void CommitTransaction(const TTransactionId& transactionId)
    {
        auto transactionSupervisor = Bootstrap_->GetTransactionSupervisor();
        WaitFor(transactionSupervisor->CommitTransaction(transactionId))
            .ThrowOnError();
    }

    void ScheduleCreateNode(
        const TYPath& path,
        const TTransactionId& transactionId,
        EObjectType type,
        const TYsonString& attributes = TYsonString("{}"))
    {
        auto service = Bootstrap_->GetObjectManager()->GetRootService();
        auto req = TCypressYPathProxy::Create(path);
        SetTransactionId(req, transactionId);
        req->set_type(static_cast<int>(type));
        req->set_recursive(true);
        ToProto(req->mutable_node_attributes(), *ConvertToAttributes(attributes));
        ScheduledMutations_.push_back(ExecuteVerb(service, req).As<void>());
    }

    void FlushScheduled()
    {
        std::vector<TFuture<void>> scheduledMutations;
        ScheduledMutations_.swap(scheduledMutations);
        WaitFor(Combine(scheduledMutations))
            .ThrowOnError();
    }

    void AbandonScheduled()
    {
        ScheduledMutations_.clear();
    }
};

////////////////////////////////////////////////////////////////////////////////

TWorldInitializer::TWorldInitializer(
    TCellMasterConfigPtr config,
    TBootstrap* bootstrap)
    : Impl_(New<TImpl>(config, bootstrap))
{ }

TWorldInitializer::~TWorldInitializer() = default;

bool TWorldInitializer::IsInitialized()
{
    return Impl_->IsInitialized();
}

void TWorldInitializer::ValidateInitialized()
{
    Impl_->ValidateInitialized();
}

bool TWorldInitializer::HasProvisionLock()
{
    return Impl_->HasProvisionLock();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NCellMaster
} // namespace NYT

