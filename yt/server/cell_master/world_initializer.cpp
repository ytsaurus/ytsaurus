#include "stdafx.h"
#include "world_initializer.h"
#include "hydra_facade.h"
#include "config.h"

#include <core/misc/collection_helpers.h>

#include <core/ytree/ypath_proxy.h>
#include <core/ytree/ypath_client.h>

#include <core/ypath/token.h>

#include <core/concurrency/scheduler.h>

#include <core/logging/log.h>

#include <ytlib/cypress_client/cypress_ypath_proxy.h>
#include <ytlib/cypress_client/rpc_helpers.h>

#include <ytlib/object_client/object_service_proxy.h>
#include <ytlib/object_client/master_ypath_proxy.h>
#include <ytlib/object_client/helpers.h>

#include <ytlib/election/cell_manager.h>

#include <ytlib/transaction_client/transaction_ypath_proxy.h>

#include <server/hive/transaction_supervisor.h>

#include <server/cell_master/bootstrap.h>

#include <server/cypress_server/cypress_manager.h>
#include <server/cypress_server/node_detail.h>

#include <server/security_server/security_manager.h>
#include <server/security_server/acl.h>
#include <server/security_server/group.h>

namespace NYT {
namespace NCellMaster {

using namespace NConcurrency;
using namespace NYTree;
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

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger Logger("Bootstrap");
static const TDuration InitRetryPeriod = TDuration::Seconds(3);
static const TDuration InitTransactionTimeout = TDuration::Seconds(60);

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


    bool IsInitialized() const
    {
        auto cypressManager = Bootstrap_->GetCypressManager();
        auto* root = dynamic_cast<TMapNode*>(cypressManager->GetRootNode());
        YCHECK(root);
        return !root->KeyToChild().empty();
    }

    void ValidateInitialized()
    {
        if (!IsInitialized()) {
            THROW_ERROR_EXCEPTION(NRpc::EErrorCode::Unavailable, "Not initialized");
        }
    }

private:
    TCellMasterConfigPtr Config_;
    TBootstrap* Bootstrap_;


    void OnLeaderActive()
    {
        // NB: Initialization cannot be carried out here since not all subsystems
        // are fully initialized yet.
        // We'll post an initialization callback to the automaton invoker instead.
        ScheduleInitialize();
    }

    void InitializeIfNeeded()
    {
        if (!IsInitialized()) {
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

            CreateNode(
                "//sys",
                transactionId,
                EObjectType::MapNode,
                BuildYsonStringFluently()
                    .BeginMap()
                        .Item("cell_tag").Value(Bootstrap_->GetCellTag())
                        .Item("cell_id").Value(Bootstrap_->GetCellId())
                    .EndMap());

            CreateNode(
                "//sys/schemas",
                transactionId,
                EObjectType::MapNode,
                BuildYsonStringFluently()
                    .BeginMap()
                        .Item("opaque").Value(true)
                    .EndMap());

            for (auto type : objectManager->GetRegisteredTypes()) {
                if (HasSchema(type)) {
                    CreateNode(
                        "//sys/schemas/" + ToYPathLiteral(FormatEnum(type)),
                        transactionId,
                        EObjectType::Link,
                        BuildYsonStringFluently()
                            .BeginMap()
                                .Item("target_id").Value(objectManager->GetSchema(type)->GetId())
                            .EndMap());
                }
            }

            CreateNode(
                "//sys/scheduler",
                transactionId,
                EObjectType::MapNode,
                BuildYsonStringFluently()
                    .BeginMap()
                        .Item("opaque").Value(true)
                    .EndMap());

            CreateNode(
                "//sys/scheduler/lock",
                transactionId,
                EObjectType::MapNode);

            CreateNode(
                "//sys/pools",
                transactionId,
                EObjectType::MapNode,
                BuildYsonStringFluently()
                    .BeginMap()
                        .Item("opaque").Value(true)
                    .EndMap());
            
            CreateNode(
                "//sys/tokens",
                transactionId,
                EObjectType::Document,
                BuildYsonStringFluently()
                    .BeginMap()
                        .Item("value").BeginMap()
                        .EndMap()
                    .EndMap());

            CreateNode(
                "//sys/clusters",
                transactionId,
                EObjectType::Document,
                BuildYsonStringFluently()
                    .BeginMap()
                        .Item("value").BeginMap()
                        .EndMap()
                    .EndMap());
            
            CreateNode(
                "//sys/empty_yamr_table",
                transactionId,
                EObjectType::Table,
                BuildYsonStringFluently()
                    .BeginMap()
                        .Item("key_columns").BeginList()
                            .Item().Value("key")
                            .Item().Value("subkey")
                        .EndList()
                    .EndMap());

            CreateNode(
                "//sys/scheduler/instances",
                transactionId,
                EObjectType::MapNode,
                BuildYsonStringFluently()
                    .BeginMap()
                        .Item("opaque").Value(true)
                    .EndMap());

            CreateNode(
                "//sys/scheduler/orchid",
                transactionId,
                EObjectType::Orchid);

            CreateNode(
                "//sys/scheduler/event_log",
                transactionId,
                EObjectType::Table);


            CreateNode(
                "//sys/operations",
                transactionId,
                EObjectType::MapNode,
                BuildYsonStringFluently()
                    .BeginMap()
                        .Item("opaque").Value(true)
                    .EndMap());

            CreateNode(
                "//sys/proxies",
                transactionId,
                EObjectType::MapNode,
                BuildYsonStringFluently()
                    .BeginMap()
                        .Item("opaque").Value(true)
                    .EndMap());

            CreateNode(
                "//sys/nodes",
                transactionId,
                EObjectType::CellNodeMap,
                BuildYsonStringFluently()
                    .BeginMap()
                        .Item("opaque").Value(true)
                    .EndMap());

            CreateNode(
                "//sys/racks",
                transactionId,
                EObjectType::RackMap);

            CreateNode(
                "//sys/masters",
                transactionId,
                EObjectType::MapNode,
                BuildYsonStringFluently()
                    .BeginMap()
                        .Item("opaque").Value(true)
                    .EndMap());

            for (const auto& address : Config_->Master->Addresses) {
                auto addressPath = "/" + ToYPathLiteral(address);

                CreateNode(
                    "//sys/masters" + addressPath,
                    transactionId,
                    EObjectType::MapNode);

                CreateNode(
                    "//sys/masters" + addressPath + "/orchid",
                    transactionId,
                    EObjectType::Orchid,
                    BuildYsonStringFluently()
                        .BeginMap()
                            .Item("remote_address").Value(address)
                        .EndMap());
            }

            CreateNode(
                "//sys/locks",
                transactionId,
                EObjectType::LockMap);

            CreateNode(
                "//sys/chunks",
                transactionId,
                EObjectType::ChunkMap);

            CreateNode(
                "//sys/lost_chunks",
                transactionId,
                EObjectType::LostChunkMap);

            CreateNode(
                "//sys/lost_vital_chunks",
                transactionId,
                EObjectType::LostVitalChunkMap);

            CreateNode(
                "//sys/overreplicated_chunks",
                transactionId,
                EObjectType::OverreplicatedChunkMap);

            CreateNode(
                "//sys/underreplicated_chunks",
                transactionId,
                EObjectType::UnderreplicatedChunkMap);

            CreateNode(
                "//sys/data_missing_chunks",
                transactionId,
                EObjectType::DataMissingChunkMap);

            CreateNode(
                "//sys/parity_missing_chunks",
                transactionId,
                EObjectType::ParityMissingChunkMap);

            CreateNode(
                "//sys/quorum_missing_chunks",
                transactionId,
                EObjectType::QuorumMissingChunkMap);

            CreateNode(
                "//sys/unsafely_placed_chunks",
                transactionId,
                EObjectType::UnsafelyPlacedChunkMap);

            CreateNode(
                "//sys/chunk_lists",
                transactionId,
                EObjectType::ChunkListMap);

            CreateNode(
                "//sys/transactions",
                transactionId,
                EObjectType::TransactionMap);

            CreateNode(
                "//sys/topmost_transactions",
                transactionId,
                EObjectType::TopmostTransactionMap);

            CreateNode(
                "//sys/accounts",
                transactionId,
                EObjectType::AccountMap);

            CreateNode(
                "//sys/users",
                transactionId,
                EObjectType::UserMap);

            CreateNode(
                "//sys/groups",
                transactionId,
                EObjectType::GroupMap);

            CreateNode(
                "//sys/tablet_cells",
                transactionId,
                EObjectType::MapNode,
                BuildYsonStringFluently()
                    .BeginMap()
                        .Item("opaque").Value(true)
                    .EndMap());

            CreateNode(
                "//sys/tablets",
                transactionId,
                EObjectType::TabletMap);

            CreateNode(
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
                                EPermissionSet(EPermission::Read | EPermission::Write)))
                        .EndList()
                    .EndMap());

            CreateNode(
                "//home",
                transactionId,
                EObjectType::MapNode,
                BuildYsonStringFluently()
                    .BeginMap()
                        .Item("opaque").Value(true)
                        .Item("account").Value("tmp")
                    .EndMap());

            CommitTransaction(transactionId);

            LOG_INFO("World initialization completed");
        } catch (const std::exception& ex) {
            LOG_ERROR(ex, "World initialization failed");
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
        auto req = TMasterYPathProxy::CreateObjects();
        req->set_type(static_cast<int>(EObjectType::Transaction));

        auto* requestExt = req->MutableExtension(TReqStartTransactionExt::create_transaction_ext);
        requestExt->set_timeout(InitTransactionTimeout.MilliSeconds());

        auto attributes = CreateEphemeralAttributes();
        attributes->Set("title", "World initialization");
        ToProto(req->mutable_object_attributes(), *attributes);

        auto rsp = WaitFor(ExecuteVerb(service, req));
        THROW_ERROR_EXCEPTION_IF_FAILED(*rsp);

        return FromProto<TTransactionId>(rsp->object_ids(0));
    }

    void CommitTransaction(const TTransactionId& transactionId)
    {
        auto transactionSupervisor = Bootstrap_->GetTransactionSupervisor();
        auto error = WaitFor(transactionSupervisor->CommitTransaction(transactionId));
        THROW_ERROR_EXCEPTION_IF_FAILED(error);
    }

    void CreateNode(
        const TYPath& path,
        const TTransactionId& transactionId,
        EObjectType type,
        const TYsonString& attributes = TYsonString("{}"))
    {
        auto service = Bootstrap_->GetObjectManager()->GetRootService();
        auto req = TCypressYPathProxy::Create(path);
        SetTransactionId(req, transactionId);
        req->set_type(static_cast<int>(type));
        ToProto(req->mutable_node_attributes(), *ConvertToAttributes(attributes));
        auto rsp = WaitFor(ExecuteVerb(service, req));
        THROW_ERROR_EXCEPTION_IF_FAILED(*rsp);
    }

};

////////////////////////////////////////////////////////////////////////////////

TWorldInitializer::TWorldInitializer(
    TCellMasterConfigPtr config,
    TBootstrap* bootstrap)
    : Impl_(New<TImpl>(config, bootstrap))
{ }

TWorldInitializer::~TWorldInitializer()
{ }

bool TWorldInitializer::IsInitialized()
{
    return Impl_->IsInitialized();
}

void TWorldInitializer::ValidateInitialized()
{
    return Impl_->ValidateInitialized();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NCellMaster
} // namespace NYT

