#include "private.h"
#include "tablet_manager.h"
#include "tablet_service.h"

#include <yt/server/master/cell_master/bootstrap.h>
#include <yt/server/master/cell_master/hydra_facade.h>

#include <yt/server/lib/hive/helpers.h>

#include <yt/server/master/table_server/shared_table_schema.h>

#include <yt/server/master/security_server/security_manager.h>

#include <yt/ytlib/tablet_client/master_tablet_service.h>

namespace NYT::NTabletServer {

using namespace NCellMaster;
using namespace NConcurrency;
using namespace NCypressClient;
using namespace NCypressServer;
using namespace NHydra;
using namespace NHiveServer;
using namespace NObjectClient;
using namespace NObjectServer;
using namespace NSecurityServer;
using namespace NTableClient;
using namespace NTableServer;
using namespace NTabletClient::NProto;
using namespace NTabletClient;
using namespace NTabletNode::NProto;
using namespace NTabletServer::NProto;
using namespace NTransactionServer;
using namespace NYPath;
using namespace NYTree;
using namespace NYson;

using NTransactionServer::TTransaction;

using NYT::FromProto;
using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = TabletServerLogger;

////////////////////////////////////////////////////////////////////////////////

class TTabletService::TImpl
    : public TMasterAutomatonPart
{
public:
    explicit TImpl(
        NCellMaster::TBootstrap* bootstrap)
        : TMasterAutomatonPart(bootstrap,  NCellMaster::EAutomatonThreadQueue::TabletManager)
    {
        VERIFY_INVOKER_THREAD_AFFINITY(Bootstrap_->GetHydraFacade()->GetAutomatonInvoker(EAutomatonThreadQueue::Default), AutomatonThread);
    }

    void Initialize()
    {
        const auto& transactionManager = Bootstrap_->GetTransactionManager();
        transactionManager->RegisterTransactionActionHandlers(
            MakeTransactionActionHandlerDescriptor(BIND(&TImpl::HydraPrepareMountTable, MakeStrong(this))),
            MakeTransactionActionHandlerDescriptor(BIND(&TImpl::HydraCommitMountTable, MakeStrong(this))),
            MakeTransactionActionHandlerDescriptor(BIND(&TImpl::HydraAbortMountTable, MakeStrong(this))));

        transactionManager->RegisterTransactionActionHandlers(
            MakeTransactionActionHandlerDescriptor(BIND(&TImpl::HydraPrepareUnmountTable, MakeStrong(this))),
            MakeTransactionActionHandlerDescriptor(BIND(&TImpl::HydraCommitUnmountTable, MakeStrong(this))),
            MakeTransactionActionHandlerDescriptor(BIND(&TImpl::HydraAbortUnmountTable, MakeStrong(this))));

        transactionManager->RegisterTransactionActionHandlers(
            MakeTransactionActionHandlerDescriptor(BIND(&TImpl::HydraPrepareFreezeTable, MakeStrong(this))),
            MakeTransactionActionHandlerDescriptor(BIND(&TImpl::HydraCommitFreezeTable, MakeStrong(this))),
            MakeTransactionActionHandlerDescriptor(BIND(&TImpl::HydraAbortFreezeTable, MakeStrong(this))));

        transactionManager->RegisterTransactionActionHandlers(
            MakeTransactionActionHandlerDescriptor(BIND(&TImpl::HydraPrepareUnfreezeTable, MakeStrong(this))),
            MakeTransactionActionHandlerDescriptor(BIND(&TImpl::HydraCommitUnfreezeTable, MakeStrong(this))),
            MakeTransactionActionHandlerDescriptor(BIND(&TImpl::HydraAbortUnfreezeTable, MakeStrong(this))));

        transactionManager->RegisterTransactionActionHandlers(
            MakeTransactionActionHandlerDescriptor(BIND(&TImpl::HydraPrepareRemountTable, MakeStrong(this))),
            MakeTransactionActionHandlerDescriptor(BIND(&TImpl::HydraCommitRemountTable, MakeStrong(this))),
            MakeTransactionActionHandlerDescriptor(BIND(&TImpl::HydraAbortRemountTable, MakeStrong(this))));

        transactionManager->RegisterTransactionActionHandlers(
            MakeTransactionActionHandlerDescriptor(BIND(&TImpl::HydraPrepareReshardTable, MakeStrong(this))),
            MakeTransactionActionHandlerDescriptor(BIND(&TImpl::HydraCommitReshardTable, MakeStrong(this))),
            MakeTransactionActionHandlerDescriptor(BIND(&TImpl::HydraAbortReshardTable, MakeStrong(this))));
    }

private:
    DECLARE_THREAD_AFFINITY_SLOT(AutomatonThread);

    static TTableNode* AsTableNodeSafe(TCypressNode* node)
    {
        if (!node) {
            return nullptr;
        }
        if (!IsTableType(node->GetType())) {
            THROW_ERROR_EXCEPTION("%v is not a table", node->GetId());
        }
        return node->As<TTableNode>();
    }

    void HydraPrepareMountTable(TTransaction* transaction, NTabletClient::NProto::TReqMount* request, bool persist)
    {
        int firstTabletIndex = request->first_tablet_index();
        int lastTabletIndex = request->last_tablet_index();
        auto hintCellId = FromProto<TTabletCellId>(request->cell_id());
        bool freeze = request->freeze();
        auto mountTimestamp = static_cast<TTimestamp>(request->mount_timestamp());
        auto tableId = FromProto<TTableId>(request->table_id());
        auto path = request->path();
        auto targetCellIds = FromProto<std::vector<TTabletCellId>>(request->target_cell_ids());

        YT_LOG_DEBUG_UNLESS(IsRecovery(), "Preparing table mount (TableId: %v, TransactionId: %v, User: %v, "
            "FirstTabletIndex: %v, LastTabletIndex: %v, CellId: %v, TargetCellIds: %v, Freeze: %v, MountTimestamp: %v)",
            tableId,
            transaction->GetId(),
            Bootstrap_->GetSecurityManager()->GetAuthenticatedUserName(),
            firstTabletIndex,
            lastTabletIndex,
            hintCellId,
            targetCellIds,
            freeze,
            mountTimestamp);

        ValidateNoParentTransaction(transaction);

        const auto& cypressManager = Bootstrap_->GetCypressManager();
        auto* table = AsTableNodeSafe(cypressManager->GetNodeOrThrow(TVersionedNodeId(tableId)));

        if (Bootstrap_->IsPrimaryMaster()) {
            auto currentPath = cypressManager->GetNodePath(table, nullptr);
            if (path != currentPath) {
                THROW_ERROR_EXCEPTION("Table path mismatch")
                    << TErrorAttribute("requested_path", path)
                    << TErrorAttribute("resolved_path", currentPath);
            }

            const auto& securityManager = Bootstrap_->GetSecurityManager();
            auto* cellBundle = table->GetTabletCellBundle();
            securityManager->ValidatePermission(cellBundle, EPermission::Use);

            // CurrentMountTransactionId is used to prevent primary master to copy/move node when
            // secondary master has already committed mount (this causes an unexpected error in CloneTable).
            // Primary master is lazy coordinator of 2pc, thus clone command and participant commit command are
            // serialized. Moreover secondary master (participant) commit happens strictly before primary commit.
            // CurrentMountTransactionId mechanism ensures that clone command can be sent only before
            // primary master has been started participating in 2pc. Thus clone command cannot appear
            // on the secondary master after commit. It can however arrive between prepare and commit
            // so we don't call this validation on secondary master. Note that this deals with
            // clone command 'before' mount. Refer to UpdateTabletState to see how we deal with it 'after' mount.
            table->ValidateNoCurrentMountTransaction("Cannot mount table");
            table->SetCurrentMountTransactionId(transaction->GetId());
        }

        cypressManager->LockNode(table, transaction, ELockMode::Exclusive, false, true);

        const auto& tabletManager = Bootstrap_->GetTabletManager();
        tabletManager->PrepareMountTable(
            table,
            firstTabletIndex,
            lastTabletIndex,
            hintCellId,
            targetCellIds,
            freeze,
            mountTimestamp);
    }

    void HydraCommitMountTable(TTransaction* transaction, NTabletClient::NProto::TReqMount* request)
    {
        int firstTabletIndex = request->first_tablet_index();
        int lastTabletIndex = request->last_tablet_index();
        auto hintCellId = FromProto<TTabletCellId>(request->cell_id());
        bool freeze = request->freeze();
        auto mountTimestamp = static_cast<TTimestamp>(request->mount_timestamp());
        auto tableId = FromProto<TTableId>(request->table_id());
        auto path = request->path();
        auto targetCellIds = FromProto<std::vector<TTabletCellId>>(request->target_cell_ids());

        YT_LOG_DEBUG_UNLESS(IsRecovery(), "Committing table mount (TableId: %v, TransactionId: %v, User: %v, "
            "FirstTabletIndex: %v, LastTabletIndex: %v, CellId: %v, TargetCellIds: %v, Freeze: %v, MountTimestamp: %v)",
            tableId,
            transaction->GetId(),
            Bootstrap_->GetSecurityManager()->GetAuthenticatedUserName(),
            firstTabletIndex,
            lastTabletIndex,
            hintCellId,
            targetCellIds,
            freeze,
            mountTimestamp);

        const auto& cypressManager = Bootstrap_->GetCypressManager();
        auto* table = AsTableNodeSafe(cypressManager->FindNode(TVersionedNodeId(tableId)));

        if (!IsObjectAlive(table)) {
            return;
        }

        if (Bootstrap_->IsPrimaryMaster()) {
            table->SetCurrentMountTransactionId(TTransactionId());
        }

        table->SetLastMountTransactionId(transaction->GetId());
        table->UpdateExpectedTabletState(freeze ? ETabletState::Frozen : ETabletState::Mounted);

        const auto& tabletManager = Bootstrap_->GetTabletManager();
        tabletManager->MountTable(
            table,
            path,
            firstTabletIndex,
            lastTabletIndex,
            hintCellId,
            targetCellIds,
            freeze,
            mountTimestamp);
    }

    void HydraAbortMountTable(TTransaction* transaction, NTabletClient::NProto::TReqMount* request)
    {
        int firstTabletIndex = request->first_tablet_index();
        int lastTabletIndex = request->last_tablet_index();
        auto hintCellId = FromProto<TTabletCellId>(request->cell_id());
        bool freeze = request->freeze();
        auto mountTimestamp = static_cast<TTimestamp>(request->mount_timestamp());
        auto tableId = FromProto<TTableId>(request->table_id());
        auto path = request->path();
        auto targetCellIds = FromProto<std::vector<TTabletCellId>>(request->target_cell_ids());

        YT_LOG_DEBUG_UNLESS(IsRecovery(), "Aborting table mount (TableId: %v, TransactionId: %v, User: %v, "
            "FirstTabletIndex: %v, LastTabletIndex: %v, CellId: %v, TargetCellIds: %v, Freeze: %v, MountTimestamp: %v)",
            tableId,
            transaction->GetId(),
            Bootstrap_->GetSecurityManager()->GetAuthenticatedUserName(),
            firstTabletIndex,
            lastTabletIndex,
            hintCellId,
            targetCellIds,
            freeze,
            mountTimestamp);

        const auto& cypressManager = Bootstrap_->GetCypressManager();
        auto* table = AsTableNodeSafe(cypressManager->FindNode(TVersionedNodeId(tableId)));

        if (!IsObjectAlive(table)) {
            return;
        }

        if (Bootstrap_->IsPrimaryMaster()) {
            table->SetCurrentMountTransactionId(TTransactionId());
        }
    }

    void HydraPrepareUnmountTable(TTransaction* transaction, NTabletClient::NProto::TReqUnmount* request, bool persist)
    {
        int firstTabletIndex = request->first_tablet_index();
        int lastTabletIndex = request->last_tablet_index();
        bool force = request->force();
        auto tableId = FromProto<TTableId>(request->table_id());

        YT_LOG_DEBUG_UNLESS(IsRecovery(), "Preparing table unmount (TableId: %v, TransactionId: %v, User: %v, "
            "Force: %v, FirstTabletIndex: %v, LastTabletIndex: %v)",
            tableId,
            transaction->GetId(),
            Bootstrap_->GetSecurityManager()->GetAuthenticatedUserName(),
            force,
            firstTabletIndex,
            lastTabletIndex);

        ValidateNoParentTransaction(transaction);

        const auto& cypressManager = Bootstrap_->GetCypressManager();
        auto* table = AsTableNodeSafe(cypressManager->GetNodeOrThrow(TVersionedNodeId(tableId)));

        if (Bootstrap_->IsPrimaryMaster()) {
            table->ValidateNoCurrentMountTransaction("Cannot unmount table");
            table->SetCurrentMountTransactionId(transaction->GetId());
        }

        cypressManager->LockNode(table, transaction, ELockMode::Exclusive, false, true);

        const auto& tabletManager = Bootstrap_->GetTabletManager();
        tabletManager->PrepareUnmountTable(
            table,
            force,
            firstTabletIndex,
            lastTabletIndex);
    }

    void HydraCommitUnmountTable(TTransaction* transaction, NTabletClient::NProto::TReqUnmount* request)
    {
        int firstTabletIndex = request->first_tablet_index();
        int lastTabletIndex = request->last_tablet_index();
        bool force = request->force();
        auto tableId = FromProto<TTableId>(request->table_id());

        YT_LOG_DEBUG_UNLESS(IsRecovery(), "Committing table unmount (TableId: %v, TransactionId: %v, User: %v, "
            "Force: %v, FirstTabletIndex: %v, LastTabletIndex: %v)",
            tableId,
            transaction->GetId(),
            Bootstrap_->GetSecurityManager()->GetAuthenticatedUserName(),
            force,
            firstTabletIndex,
            lastTabletIndex);

        const auto& cypressManager = Bootstrap_->GetCypressManager();
        auto* table = AsTableNodeSafe(cypressManager->FindNode(TVersionedNodeId(tableId)));

        if (!IsObjectAlive(table)) {
            return;
        }

        if (Bootstrap_->IsPrimaryMaster()) {
            table->SetCurrentMountTransactionId(TTransactionId());
        }

        table->SetLastMountTransactionId(transaction->GetId());

        const auto& tabletManager = Bootstrap_->GetTabletManager();
        tabletManager->UnmountTable(
            table,
            force,
            firstTabletIndex,
            lastTabletIndex);
    }

    void HydraAbortUnmountTable(TTransaction* transaction, NTabletClient::NProto::TReqUnmount* request)
    {
        int firstTabletIndex = request->first_tablet_index();
        int lastTabletIndex = request->last_tablet_index();
        bool force = request->force();
        auto tableId = FromProto<TTableId>(request->table_id());

        YT_LOG_DEBUG_UNLESS(IsRecovery(), "Aborting table unmount (TableId: %v, TransactionId: %v, User: %v, "
            "Force: %v, FirstTabletIndex: %v, LastTabletIndex: %v)",
            tableId,
            transaction->GetId(),
            Bootstrap_->GetSecurityManager()->GetAuthenticatedUserName(),
            force,
            firstTabletIndex,
            lastTabletIndex);

        const auto& cypressManager = Bootstrap_->GetCypressManager();
        auto* table = AsTableNodeSafe(cypressManager->FindNode(TVersionedNodeId(tableId)));

        if (!IsObjectAlive(table)) {
            return;
        }

        if (Bootstrap_->IsPrimaryMaster()) {
            table->SetCurrentMountTransactionId(TTransactionId());
        }
    }

    void HydraPrepareFreezeTable(TTransaction* transaction, NTabletClient::NProto::TReqFreeze* request, bool persist)
    {
        int firstTabletIndex = request->first_tablet_index();
        int lastTabletIndex = request->last_tablet_index();
        auto tableId = FromProto<TTableId>(request->table_id());

        YT_LOG_DEBUG_UNLESS(IsRecovery(), "Preparing table freeze (TableId: %v, TransactionId: %v, User: %v, "
            "FirstTabletIndex: %v, LastTabletIndex: %v)",
            tableId,
            transaction->GetId(),
            Bootstrap_->GetSecurityManager()->GetAuthenticatedUserName(),
            firstTabletIndex,
            lastTabletIndex);

        ValidateNoParentTransaction(transaction);

        const auto& cypressManager = Bootstrap_->GetCypressManager();
        auto* table = AsTableNodeSafe(cypressManager->GetNodeOrThrow(TVersionedNodeId(tableId)));

        if (Bootstrap_->IsPrimaryMaster()) {
            table->ValidateNoCurrentMountTransaction("Cannot freeze table");
            table->SetCurrentMountTransactionId(transaction->GetId());
        }

        cypressManager->LockNode(table, transaction, ELockMode::Exclusive, false, true);

        const auto& tabletManager = Bootstrap_->GetTabletManager();
        tabletManager->PrepareFreezeTable(
            table,
            firstTabletIndex,
            lastTabletIndex);
    }

    void HydraCommitFreezeTable(TTransaction* transaction, NTabletClient::NProto::TReqFreeze* request)
    {
        int firstTabletIndex = request->first_tablet_index();
        int lastTabletIndex = request->last_tablet_index();
        auto tableId = FromProto<TTableId>(request->table_id());

        YT_LOG_DEBUG_UNLESS(IsRecovery(), "Committing table freeze (TableId: %v, TransactionId: %v, User: %v, "
            "FirstTabletIndex: %v, LastTabletIndex: %v)",
            tableId,
            transaction->GetId(),
            Bootstrap_->GetSecurityManager()->GetAuthenticatedUserName(),
            firstTabletIndex,
            lastTabletIndex);

        const auto& cypressManager = Bootstrap_->GetCypressManager();
        auto* table = AsTableNodeSafe(cypressManager->FindNode(TVersionedNodeId(tableId)));

        if (!IsObjectAlive(table)) {
            return;
        }

        if (Bootstrap_->IsPrimaryMaster()) {
            table->SetCurrentMountTransactionId(TTransactionId());
        }

        table->SetLastMountTransactionId(transaction->GetId());

        const auto& tabletManager = Bootstrap_->GetTabletManager();
        tabletManager->FreezeTable(
            table,
            firstTabletIndex,
            lastTabletIndex);
    }

    void HydraAbortFreezeTable(TTransaction* transaction, NTabletClient::NProto::TReqFreeze* request)
    {
        int firstTabletIndex = request->first_tablet_index();
        int lastTabletIndex = request->last_tablet_index();
        auto tableId = FromProto<TTableId>(request->table_id());

        YT_LOG_DEBUG_UNLESS(IsRecovery(), "Aborting table freeze (TableId: %v, TransactionId: %v, User: %v, "
            "FirstTabletIndex: %v, LastTabletIndex: %v)",
            tableId,
            transaction->GetId(),
            Bootstrap_->GetSecurityManager()->GetAuthenticatedUserName(),
            firstTabletIndex,
            lastTabletIndex);

        const auto& cypressManager = Bootstrap_->GetCypressManager();
        auto* table = AsTableNodeSafe(cypressManager->FindNode(TVersionedNodeId(tableId)));

        if (!IsObjectAlive(table)) {
            return;
        }

        if (Bootstrap_->IsPrimaryMaster()) {
            table->SetCurrentMountTransactionId(TTransactionId());
        }
    }

    void HydraPrepareUnfreezeTable(TTransaction* transaction, NTabletClient::NProto::TReqUnfreeze* request, bool persist)
    {
        int firstTabletIndex = request->first_tablet_index();
        int lastTabletIndex = request->last_tablet_index();
        auto tableId = FromProto<TTableId>(request->table_id());

        YT_LOG_DEBUG_UNLESS(IsRecovery(), "Preparing table unfreeze (TableId: %v, TransactionId: %v, User: %v, "
            "FirstTabletIndex: %v, LastTabletIndex: %v)",
            tableId,
            transaction->GetId(),
            Bootstrap_->GetSecurityManager()->GetAuthenticatedUserName(),
            firstTabletIndex,
            lastTabletIndex);

        ValidateNoParentTransaction(transaction);

        const auto& cypressManager = Bootstrap_->GetCypressManager();
        auto* table = AsTableNodeSafe(cypressManager->GetNodeOrThrow(TVersionedNodeId(tableId)));

        if (Bootstrap_->IsPrimaryMaster()) {
            table->ValidateNoCurrentMountTransaction("Cannot unfreeze table");
            table->SetCurrentMountTransactionId(transaction->GetId());
        }

        cypressManager->LockNode(table, transaction, ELockMode::Exclusive, false, true);

        const auto& tabletManager = Bootstrap_->GetTabletManager();
        tabletManager->PrepareUnfreezeTable(
            table,
            firstTabletIndex,
            lastTabletIndex);
    }

    void HydraCommitUnfreezeTable(TTransaction* transaction, NTabletClient::NProto::TReqUnfreeze* request)
    {
        int firstTabletIndex = request->first_tablet_index();
        int lastTabletIndex = request->last_tablet_index();
        auto tableId = FromProto<TTableId>(request->table_id());

        YT_LOG_DEBUG_UNLESS(IsRecovery(), "Committing table unfreeze (TableId: %v, TransactionId: %v, User: %v, "
            "FirstTabletIndex: %v, LastTabletIndex: %v)",
            tableId,
            transaction->GetId(),
            Bootstrap_->GetSecurityManager()->GetAuthenticatedUserName(),
            firstTabletIndex,
            lastTabletIndex);

        const auto& cypressManager = Bootstrap_->GetCypressManager();
        auto* table = AsTableNodeSafe(cypressManager->FindNode(TVersionedNodeId(tableId)));

        if (!IsObjectAlive(table)) {
            return;
        }

        if (Bootstrap_->IsPrimaryMaster()) {
            table->SetCurrentMountTransactionId(TTransactionId());
        }

        table->SetLastMountTransactionId(transaction->GetId());
        table->UpdateExpectedTabletState(ETabletState::Mounted);

        const auto& tabletManager = Bootstrap_->GetTabletManager();
        tabletManager->UnfreezeTable(
            table,
            firstTabletIndex,
            lastTabletIndex);
    }

    void HydraAbortUnfreezeTable(TTransaction* transaction, NTabletClient::NProto::TReqUnfreeze* request)
    {
        int firstTabletIndex = request->first_tablet_index();
        int lastTabletIndex = request->last_tablet_index();
        auto tableId = FromProto<TTableId>(request->table_id());

        YT_LOG_DEBUG_UNLESS(IsRecovery(), "Aborting table unfreeze (TableId: %v, TransactionId: %v, User: %v, "
            "FirstTabletIndex: %v, LastTabletIndex: %v)",
            tableId,
            transaction->GetId(),
            Bootstrap_->GetSecurityManager()->GetAuthenticatedUserName(),
            firstTabletIndex,
            lastTabletIndex);

        const auto& cypressManager = Bootstrap_->GetCypressManager();
        auto* table = AsTableNodeSafe(cypressManager->FindNode(TVersionedNodeId(tableId)));

        if (!IsObjectAlive(table)) {
            return;
        }

        if (Bootstrap_->IsPrimaryMaster()) {
            table->SetCurrentMountTransactionId(TTransactionId());
        }
    }

    void HydraPrepareRemountTable(TTransaction* transaction, NTabletClient::NProto::TReqRemount* request, bool persist)
    {
        int firstTabletIndex = request->first_tablet_index();
        int lastTabletIndex = request->last_tablet_index();
        auto tableId = FromProto<TTableId>(request->table_id());

        YT_LOG_DEBUG_UNLESS(IsRecovery(), "Preparing table remount (TableId: %v, TransactionId: %v, User: %v, "
            "FirstTabletIndex: %v, LastTabletIndex: %v)",
            tableId,
            transaction->GetId(),
            Bootstrap_->GetSecurityManager()->GetAuthenticatedUserName(),
            firstTabletIndex,
            lastTabletIndex);

        ValidateNoParentTransaction(transaction);

        const auto& cypressManager = Bootstrap_->GetCypressManager();
        auto* table = AsTableNodeSafe(cypressManager->GetNodeOrThrow(TVersionedNodeId(tableId)));

        if (Bootstrap_->IsPrimaryMaster()) {
            table->ValidateNoCurrentMountTransaction("Cannot remount table");
            table->SetCurrentMountTransactionId(transaction->GetId());
        }

        cypressManager->LockNode(table, transaction, ELockMode::Exclusive, false, true);

        const auto& tabletManager = Bootstrap_->GetTabletManager();
        tabletManager->PrepareRemountTable(
            table,
            firstTabletIndex,
            lastTabletIndex);
    }

    void HydraCommitRemountTable(TTransaction* transaction, NTabletClient::NProto::TReqRemount* request)
    {
        int firstTabletIndex = request->first_tablet_index();
        int lastTabletIndex = request->last_tablet_index();
        auto tableId = FromProto<TTableId>(request->table_id());

        YT_LOG_DEBUG_UNLESS(IsRecovery(), "Committing table remount (TableId: %v, TransactionId: %v, User: %v, "
            "FirstTabletIndex: %v, LastTabletIndex: %v)",
            tableId,
            transaction->GetId(),
            Bootstrap_->GetSecurityManager()->GetAuthenticatedUserName(),
            firstTabletIndex,
            lastTabletIndex);

        const auto& cypressManager = Bootstrap_->GetCypressManager();
        auto* table = AsTableNodeSafe(cypressManager->FindNode(TVersionedNodeId(tableId)));

        if (!IsObjectAlive(table)) {
            return;
        }

        if (Bootstrap_->IsPrimaryMaster()) {
            table->SetCurrentMountTransactionId(TTransactionId());
        }

        const auto& tabletManager = Bootstrap_->GetTabletManager();
        tabletManager->RemountTable(
            table,
            firstTabletIndex,
            lastTabletIndex);
    }

    void HydraAbortRemountTable(TTransaction* transaction, NTabletClient::NProto::TReqRemount* request)
    {
        int firstTabletIndex = request->first_tablet_index();
        int lastTabletIndex = request->last_tablet_index();
        auto tableId = FromProto<TTableId>(request->table_id());

        YT_LOG_DEBUG_UNLESS(IsRecovery(), "Aborting table remount (TableId: %v, TransactionId: %v, User: %v, "
            "FirstTabletIndex: %v, LastTabletIndex: %v)",
            tableId,
            transaction->GetId(),
            Bootstrap_->GetSecurityManager()->GetAuthenticatedUserName(),
            firstTabletIndex,
            lastTabletIndex);

        const auto& cypressManager = Bootstrap_->GetCypressManager();
        auto* table = AsTableNodeSafe(cypressManager->FindNode(TVersionedNodeId(tableId)));

        if (!IsObjectAlive(table)) {
            return;
        }

        if (Bootstrap_->IsPrimaryMaster()) {
            table->SetCurrentMountTransactionId(TTransactionId());
        }
    }

    void HydraPrepareReshardTable(TTransaction* transaction, NTabletClient::NProto::TReqReshard* request, bool persist)
    {
        int firstTabletIndex = request->first_tablet_index();
        int lastTabletIndex = request->last_tablet_index();
        int tabletCount = request->tablet_count();
        auto pivotKeys = FromProto<std::vector<TOwningKey>>(request->pivot_keys());
        auto tableId = FromProto<TTableId>(request->table_id());

        YT_LOG_DEBUG_UNLESS(IsRecovery(), "Preparing table reshard (TableId: %v, TransactionId: %v, User: %v, "
            "TabletCount: %v, PivotKeysSize: %v, FirstTabletIndex: %v, LastTabletIndex: %v)",
            tableId,
            transaction->GetId(),
            Bootstrap_->GetSecurityManager()->GetAuthenticatedUserName(),
            tabletCount,
            pivotKeys.size(),
            firstTabletIndex,
            lastTabletIndex);

        ValidateNoParentTransaction(transaction);

        const auto& cypressManager = Bootstrap_->GetCypressManager();
        auto* table = AsTableNodeSafe(cypressManager->GetNodeOrThrow(TVersionedNodeId(tableId)));

        if (Bootstrap_->IsPrimaryMaster()) {
            table->ValidateNoCurrentMountTransaction("Cannot reshard table");
            table->SetCurrentMountTransactionId(transaction->GetId());
        }

        cypressManager->LockNode(table, transaction, ELockMode::Exclusive, false, true);

        if (!table->IsDynamic()) {
            THROW_ERROR_EXCEPTION("Cannot reshard a static table");
        }

        const auto& tabletManager = Bootstrap_->GetTabletManager();
        tabletManager->PrepareReshardTable(
            table,
            firstTabletIndex,
            lastTabletIndex,
            tabletCount,
            pivotKeys);
    }

    void HydraCommitReshardTable(TTransaction* transaction, NTabletClient::NProto::TReqReshard* request)
    {
        int firstTabletIndex = request->first_tablet_index();
        int lastTabletIndex = request->last_tablet_index();
        int tabletCount = request->tablet_count();
        auto pivotKeys = FromProto<std::vector<TOwningKey>>(request->pivot_keys());
        auto tableId = FromProto<TTableId>(request->table_id());

        YT_LOG_DEBUG_UNLESS(IsRecovery(), "Committing table reshard (TableId: %v, TransactionId: %v, User: %v, "
            "TabletCount: %v, PivotKeysSize: %v, FirstTabletIndex: %v, LastTabletIndex: %v)",
            tableId,
            transaction->GetId(),
            Bootstrap_->GetSecurityManager()->GetAuthenticatedUserName(),
            tabletCount,
            pivotKeys.size(),
            firstTabletIndex,
            lastTabletIndex);

        const auto& cypressManager = Bootstrap_->GetCypressManager();
        auto* table = AsTableNodeSafe(cypressManager->FindNode(TVersionedNodeId(tableId)));

        if (!IsObjectAlive(table)) {
            return;
        }

        if (Bootstrap_->IsPrimaryMaster()) {
            table->SetCurrentMountTransactionId(TTransactionId());
        }

        table->SetLastMountTransactionId(transaction->GetId());

        const auto& tabletManager = Bootstrap_->GetTabletManager();
        tabletManager->ReshardTable(
            table,
            firstTabletIndex,
            lastTabletIndex,
            tabletCount,
            pivotKeys);
    }

    void HydraAbortReshardTable(TTransaction* transaction, NTabletClient::NProto::TReqReshard* request)
    {
        int firstTabletIndex = request->first_tablet_index();
        int lastTabletIndex = request->last_tablet_index();
        int tabletCount = request->tablet_count();
        auto pivotKeys = FromProto<std::vector<TOwningKey>>(request->pivot_keys());
        auto tableId = FromProto<TTableId>(request->table_id());

        YT_LOG_DEBUG_UNLESS(IsRecovery(), "Aborting table reshard (TableId: %v, TransactionId: %v, User: %v, "
            "TabletCount: %v, PivotKeysSize: %v, FirstTabletIndex: %v, LastTabletIndex: %v)",
            tableId,
            transaction->GetId(),
            Bootstrap_->GetSecurityManager()->GetAuthenticatedUserName(),
            tabletCount,
            pivotKeys.size(),
            firstTabletIndex,
            lastTabletIndex);

        const auto& cypressManager = Bootstrap_->GetCypressManager();
        auto* table = AsTableNodeSafe(cypressManager->FindNode(TVersionedNodeId(tableId)));

        if (!IsObjectAlive(table)) {
            return;
        }

        if (Bootstrap_->IsPrimaryMaster()) {
            table->SetCurrentMountTransactionId(TTransactionId());
        }
    }

    void ValidateNoParentTransaction(TTransaction* transaction)
    {
        if (transaction->GetParent()) {
            THROW_ERROR_EXCEPTION("Operation cannot be performed in transaction");
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

TTabletService::TTabletService(
    NCellMaster::TBootstrap* bootstrap)
    : Impl_(New<TImpl>(bootstrap))
{ }

void TTabletService::Initialize()
{
    Impl_->Initialize();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletServer

