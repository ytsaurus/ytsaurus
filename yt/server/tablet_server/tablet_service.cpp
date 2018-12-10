#include "private.h"
#include "tablet_manager.h"
#include "tablet_service.h"

#include <yt/server/cell_master/bootstrap.h>
#include <yt/server/cell_master/hydra_facade.h>

#include <yt/server/hive/helpers.h>

#include <yt/server/table_server/shared_table_schema.h>

#include <yt/server/security_server/security_manager.h>

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
            MakeTransactionActionHandlerDescriptor(MakeEmptyTransactionActionHandler<TTransaction, NTabletClient::NProto::TReqMount>()));

        transactionManager->RegisterTransactionActionHandlers(
            MakeTransactionActionHandlerDescriptor(BIND(&TImpl::HydraPrepareUnmountTable, MakeStrong(this))),
            MakeTransactionActionHandlerDescriptor(BIND(&TImpl::HydraCommitUnmountTable, MakeStrong(this))),
            MakeTransactionActionHandlerDescriptor(MakeEmptyTransactionActionHandler<TTransaction, NTabletClient::NProto::TReqUnmount>()));

        transactionManager->RegisterTransactionActionHandlers(
            MakeTransactionActionHandlerDescriptor(BIND(&TImpl::HydraPrepareFreezeTable, MakeStrong(this))),
            MakeTransactionActionHandlerDescriptor(BIND(&TImpl::HydraCommitFreezeTable, MakeStrong(this))),
            MakeTransactionActionHandlerDescriptor(MakeEmptyTransactionActionHandler<TTransaction, NTabletClient::NProto::TReqFreeze>()));

        transactionManager->RegisterTransactionActionHandlers(
            MakeTransactionActionHandlerDescriptor(BIND(&TImpl::HydraPrepareUnfreezeTable, MakeStrong(this))),
            MakeTransactionActionHandlerDescriptor(BIND(&TImpl::HydraCommitUnfreezeTable, MakeStrong(this))),
            MakeTransactionActionHandlerDescriptor(MakeEmptyTransactionActionHandler<TTransaction, NTabletClient::NProto::TReqUnfreeze>()));

        transactionManager->RegisterTransactionActionHandlers(
            MakeTransactionActionHandlerDescriptor(BIND(&TImpl::HydraPrepareRemountTable, MakeStrong(this))),
            MakeTransactionActionHandlerDescriptor(BIND(&TImpl::HydraCommitRemountTable, MakeStrong(this))),
            MakeTransactionActionHandlerDescriptor(MakeEmptyTransactionActionHandler<TTransaction, NTabletClient::NProto::TReqRemount>()));

        transactionManager->RegisterTransactionActionHandlers(
            MakeTransactionActionHandlerDescriptor(BIND(&TImpl::HydraPrepareReshardTable, MakeStrong(this))),
            MakeTransactionActionHandlerDescriptor(BIND(&TImpl::HydraCommitReshardTable, MakeStrong(this))),
            MakeTransactionActionHandlerDescriptor(MakeEmptyTransactionActionHandler<TTransaction, NTabletClient::NProto::TReqReshard>()));
    }

private:
    DECLARE_THREAD_AFFINITY_SLOT(AutomatonThread);

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

        LOG_DEBUG_UNLESS(IsRecovery(), "Preparing table mount (TableId: %v, TransactionId: %v, User: %v, "
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
        auto* table = cypressManager->GetNodeOrThrow(TVersionedNodeId(tableId))->As<TTableNode>();

        ValidateMountPermissions(table);

        if (Bootstrap_->IsPrimaryMaster()) {
            auto currentPath = cypressManager->GetNodePath(table, nullptr);
            if (path != currentPath) {
                THROW_ERROR_EXCEPTION("Table path mismatch")
                    << TErrorAttribute("requested_path", path)
                    << TErrorAttribute("resolved_path", currentPath);
            }
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

        LOG_DEBUG_UNLESS(IsRecovery(), "Committing table mount (TableId: %v, TransactionId: %v, User: %v, "
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
        auto* table = cypressManager->FindNode(TVersionedNodeId(tableId))->As<TTableNode>();

        if (!IsObjectAlive(table)) {
            return;
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

    void HydraPrepareUnmountTable(TTransaction* transaction, NTabletClient::NProto::TReqUnmount* request, bool persist)
    {
        int firstTabletIndex = request->first_tablet_index();
        int lastTabletIndex = request->last_tablet_index();
        bool force = request->force();
        auto tableId = FromProto<TTableId>(request->table_id());

        LOG_DEBUG_UNLESS(IsRecovery(), "Preparing table unmount (TableId: %v, TransactionId: %v, User: %v, "
            "Force: %v, FirstTabletIndex: %v, LastTabletIndex: %v)",
            tableId,
            transaction->GetId(),
            Bootstrap_->GetSecurityManager()->GetAuthenticatedUserName(),
            force,
            firstTabletIndex,
            lastTabletIndex);

        ValidateNoParentTransaction(transaction);

        const auto& cypressManager = Bootstrap_->GetCypressManager();
        auto* table = cypressManager->GetNodeOrThrow(TVersionedNodeId(tableId))->As<TTableNode>();

        ValidateMountPermissions(table);

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

        LOG_DEBUG_UNLESS(IsRecovery(), "Committing table unmount (TableId: %v, TransactionId: %v, User: %v, "
            "Force: %v, FirstTabletIndex: %v, LastTabletIndex: %v)",
            tableId,
            transaction->GetId(),
            Bootstrap_->GetSecurityManager()->GetAuthenticatedUserName(),
            force,
            firstTabletIndex,
            lastTabletIndex);

        const auto& cypressManager = Bootstrap_->GetCypressManager();
        auto* table = cypressManager->FindNode(TVersionedNodeId(tableId))->As<TTableNode>();

        if (!IsObjectAlive(table)) {
            return;
        }

        table->SetLastMountTransactionId(transaction->GetId());

        const auto& tabletManager = Bootstrap_->GetTabletManager();
        tabletManager->UnmountTable(
            table,
            force,
            firstTabletIndex,
            lastTabletIndex); 
    }

    void HydraPrepareFreezeTable(TTransaction* transaction, NTabletClient::NProto::TReqFreeze* request, bool persist)
    {
        int firstTabletIndex = request->first_tablet_index();
        int lastTabletIndex = request->last_tablet_index();
        auto tableId = FromProto<TTableId>(request->table_id());

        LOG_DEBUG_UNLESS(IsRecovery(), "Preparing table freeze (TableId: %v, TransactionId: %v, User: %v, "
            "FirstTabletIndex: %v, LastTabletIndex: %v)",
            tableId,
            transaction->GetId(),
            Bootstrap_->GetSecurityManager()->GetAuthenticatedUserName(),
            firstTabletIndex,
            lastTabletIndex);

        ValidateNoParentTransaction(transaction);

        const auto& cypressManager = Bootstrap_->GetCypressManager();
        auto* table = cypressManager->GetNodeOrThrow(TVersionedNodeId(tableId))->As<TTableNode>();

        ValidateMountPermissions(table);

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

        LOG_DEBUG_UNLESS(IsRecovery(), "Committing table freeze (TableId: %v, TransactionId: %v, User: %v, "
            "FirstTabletIndex: %v, LastTabletIndex: %v)",
            tableId,
            transaction->GetId(),
            Bootstrap_->GetSecurityManager()->GetAuthenticatedUserName(),
            firstTabletIndex,
            lastTabletIndex);

        const auto& cypressManager = Bootstrap_->GetCypressManager();
        auto* table = cypressManager->FindNode(TVersionedNodeId(tableId))->As<TTableNode>();

        if (!IsObjectAlive(table)) {
            return;
        }

        table->SetLastMountTransactionId(transaction->GetId());

        const auto& tabletManager = Bootstrap_->GetTabletManager();
        tabletManager->FreezeTable(
            table,
            firstTabletIndex,
            lastTabletIndex); 
    }

    void HydraPrepareUnfreezeTable(TTransaction* transaction, NTabletClient::NProto::TReqUnfreeze* request, bool persist)
    {
        int firstTabletIndex = request->first_tablet_index();
        int lastTabletIndex = request->last_tablet_index();
        auto tableId = FromProto<TTableId>(request->table_id());

        LOG_DEBUG_UNLESS(IsRecovery(), "Preparing table unfreeze (TableId: %v, TransactionId: %v, User: %v, "
            "FirstTabletIndex: %v, LastTabletIndex: %v)",
            tableId,
            transaction->GetId(),
            Bootstrap_->GetSecurityManager()->GetAuthenticatedUserName(),
            firstTabletIndex,
            lastTabletIndex);

        ValidateNoParentTransaction(transaction);

        const auto& cypressManager = Bootstrap_->GetCypressManager();
        auto* table = cypressManager->GetNodeOrThrow(TVersionedNodeId(tableId))->As<TTableNode>();

        ValidateMountPermissions(table);

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

        LOG_DEBUG_UNLESS(IsRecovery(), "Committing table unfreeze (TableId: %v, TransactionId: %v, User: %v, "
            "FirstTabletIndex: %v, LastTabletIndex: %v)",
            tableId,
            transaction->GetId(),
            Bootstrap_->GetSecurityManager()->GetAuthenticatedUserName(),
            firstTabletIndex,
            lastTabletIndex);

        const auto& cypressManager = Bootstrap_->GetCypressManager();
        auto* table = cypressManager->FindNode(TVersionedNodeId(tableId))->As<TTableNode>();

        if (!IsObjectAlive(table)) {
            return;
        }

        table->SetLastMountTransactionId(transaction->GetId());
        table->UpdateExpectedTabletState(ETabletState::Mounted);

        const auto& tabletManager = Bootstrap_->GetTabletManager();
        tabletManager->UnfreezeTable(
            table,
            firstTabletIndex,
            lastTabletIndex); 
    }

    void HydraPrepareRemountTable(TTransaction* transaction, NTabletClient::NProto::TReqRemount* request, bool persist)
    {
        int firstTabletIndex = request->first_tablet_index();
        int lastTabletIndex = request->last_tablet_index();
        auto tableId = FromProto<TTableId>(request->table_id());

        LOG_DEBUG_UNLESS(IsRecovery(), "Preparing table remount (TableId: %v, TransactionId: %v, User: %v, "
            "FirstTabletIndex: %v, LastTabletIndex: %v)",
            tableId,
            transaction->GetId(),
            Bootstrap_->GetSecurityManager()->GetAuthenticatedUserName(),
            firstTabletIndex,
            lastTabletIndex);

        ValidateNoParentTransaction(transaction);

        const auto& cypressManager = Bootstrap_->GetCypressManager();
        auto* table = cypressManager->GetNodeOrThrow(TVersionedNodeId(tableId))->As<TTableNode>();

        ValidateMountPermissions(table);

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

        LOG_DEBUG_UNLESS(IsRecovery(), "Committing table remount (TableId: %v, TransactionId: %v, User: %v, "
            "FirstTabletIndex: %v, LastTabletIndex: %v)",
            tableId,
            transaction->GetId(),
            Bootstrap_->GetSecurityManager()->GetAuthenticatedUserName(),
            firstTabletIndex,
            lastTabletIndex);

        const auto& cypressManager = Bootstrap_->GetCypressManager();
        auto* table = cypressManager->FindNode(TVersionedNodeId(tableId))->As<TTableNode>();

        if (!IsObjectAlive(table)) {
            return;
        }

        const auto& tabletManager = Bootstrap_->GetTabletManager();
        tabletManager->RemountTable(
            table,
            firstTabletIndex,
            lastTabletIndex); 
    }

    void HydraPrepareReshardTable(TTransaction* transaction, NTabletClient::NProto::TReqReshard* request, bool persist)
    {
        int firstTabletIndex = request->first_tablet_index();
        int lastTabletIndex = request->last_tablet_index();
        int tabletCount = request->tablet_count();
        auto pivotKeys = FromProto<std::vector<TOwningKey>>(request->pivot_keys());
        auto tableId = FromProto<TTableId>(request->table_id());

        LOG_DEBUG_UNLESS(IsRecovery(), "Preparing table reshard (TableId: %v, TransactionId: %v, User: %v, "
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
        auto* table = cypressManager->GetNodeOrThrow(TVersionedNodeId(tableId))->As<TTableNode>();

        ValidateMountPermissions(table);

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

        LOG_DEBUG_UNLESS(IsRecovery(), "Committing table reshard (TableId: %v, TransactionId: %v, User: %v, "
            "TabletCount: %v, PivotKeysSize: %v, FirstTabletIndex: %v, LastTabletIndex: %v)",
            tableId,
            transaction->GetId(),
            Bootstrap_->GetSecurityManager()->GetAuthenticatedUserName(),
            tabletCount,
            pivotKeys.size(),
            firstTabletIndex,
            lastTabletIndex);

        const auto& cypressManager = Bootstrap_->GetCypressManager();
        auto* table = cypressManager->FindNode(TVersionedNodeId(tableId))->As<TTableNode>();

        if (!IsObjectAlive(table)) {
            return;
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

    void ValidateNoParentTransaction(TTransaction* transaction)
    {
        if (transaction->GetParent()) {
            THROW_ERROR_EXCEPTION("Operation cannot be performed in transaction");
        }
    }

    void ValidateMountPermissions(TTableNode* table)
    {
        if (table->IsForeign()) {
            return;
        }

        const auto& securityManager = Bootstrap_->GetSecurityManager();
        auto* user = securityManager->GetAuthenticatedUser();
        securityManager->ValidatePermission(table, user, EPermission::Mount);
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

