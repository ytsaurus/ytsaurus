#include "private.h"
#include "tablet_manager.h"
#include "tablet_service.h"

#include <yt/server/cell_master/bootstrap.h>
#include <yt/server/cell_master/hydra_facade.h>

#include <yt/server/hive/helpers.h>

#include <yt/server/table_server/shared_table_schema.h>

#include <yt/ytlib/table_client/table_ypath_proxy.h>

namespace NYT {
namespace NTabletServer {

using namespace NCellMaster;
using namespace NConcurrency;
using namespace NCypressClient;
using namespace NCypressServer;
using namespace NHydra;
using namespace NHiveServer;
using namespace NObjectClient;
using namespace NObjectServer;
using namespace NSecurityServer;
using namespace NTableClient::NProto;
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
        transactionManager->RegisterPrepareActionHandler(MakeTransactionActionHandlerDescriptor(BIND(&TImpl::HydraPrepareMountTable, MakeStrong(this))));
        transactionManager->RegisterCommitActionHandler(MakeTransactionActionHandlerDescriptor(BIND(&TImpl::HydraCommitMountTable, MakeStrong(this))));

        transactionManager->RegisterPrepareActionHandler(MakeTransactionActionHandlerDescriptor(BIND(&TImpl::HydraPrepareUnmountTable, MakeStrong(this))));
        transactionManager->RegisterCommitActionHandler(MakeTransactionActionHandlerDescriptor(BIND(&TImpl::HydraCommitUnmountTable, MakeStrong(this))));

        transactionManager->RegisterPrepareActionHandler(MakeTransactionActionHandlerDescriptor(BIND(&TImpl::HydraPrepareFreezeTable, MakeStrong(this))));
        transactionManager->RegisterCommitActionHandler(MakeTransactionActionHandlerDescriptor(BIND(&TImpl::HydraCommitFreezeTable, MakeStrong(this))));

        transactionManager->RegisterPrepareActionHandler(MakeTransactionActionHandlerDescriptor(BIND(&TImpl::HydraPrepareUnfreezeTable, MakeStrong(this))));
        transactionManager->RegisterCommitActionHandler(MakeTransactionActionHandlerDescriptor(BIND(&TImpl::HydraCommitUnfreezeTable, MakeStrong(this))));

        transactionManager->RegisterPrepareActionHandler(MakeTransactionActionHandlerDescriptor(BIND(&TImpl::HydraPrepareRemountTable, MakeStrong(this))));
        transactionManager->RegisterCommitActionHandler(MakeTransactionActionHandlerDescriptor(BIND(&TImpl::HydraCommitRemountTable, MakeStrong(this))));

        transactionManager->RegisterPrepareActionHandler(MakeTransactionActionHandlerDescriptor(BIND(&TImpl::HydraPrepareReshardTable, MakeStrong(this))));
        transactionManager->RegisterCommitActionHandler(MakeTransactionActionHandlerDescriptor(BIND(&TImpl::HydraCommitReshardTable, MakeStrong(this))));
    }

private:
    DECLARE_THREAD_AFFINITY_SLOT(AutomatonThread);

    void HydraPrepareMountTable(TTransaction* transaction, NTableClient::NProto::TReqMount* request, bool persist)
    {
        int firstTabletIndex = request->first_tablet_index();
        int lastTabletIndex = request->last_tablet_index();
        auto cellId = FromProto<TTabletCellId>(request->cell_id());
        bool freeze = request->freeze();
        auto mountTimestamp = static_cast<TTimestamp>(request->mount_timestamp());
        auto tableId = FromProto<TTableId>(request->table_id());
        auto path = request->path();

        LOG_DEBUG_UNLESS(IsRecovery(), "Prepare mount table (TableId: %v, TransactionId: %v, User: %v, "
            "FirstTabletIndex: %v, LastTabletIndex: %v, CellId: %v, Freeze: %v, MountTimestamp: %v)",
            tableId,
            transaction->GetId(),
            Bootstrap_->GetSecurityManager()->GetAuthenticatedUserName(),
            firstTabletIndex,
            lastTabletIndex,
            cellId,
            freeze,
            mountTimestamp);

        ValidateNoParentTransaction(transaction);

        const auto& cypressManager = Bootstrap_->GetCypressManager();
        auto* table = cypressManager->GetNodeOrThrow(TVersionedNodeId(tableId))->As<TTableNode>();
        auto proxy = cypressManager->GetNodeProxy(table, transaction);
        proxy->CypressValidatePermission(EPermissionCheckScope::This, EPermission::Mount);

        if (Bootstrap_->IsPrimaryMaster()) {
            auto currentPath = cypressManager->GetNodePath(table, nullptr);
            if (path != currentPath) {
                THROW_ERROR_EXCEPTION("Table path mismatch")
                    << TErrorAttribute("requested_path", path)
                    << TErrorAttribute("resolved_path", currentPath);
            }
        }

        cypressManager->LockNode(table, transaction, ELockMode::Exclusive, false, true);

        if (table->IsExternal()) {
            return;
        }

        const auto& tabletManager = Bootstrap_->GetTabletManager();

        TTabletCell* cell = nullptr;
        if (cellId) {
            cell = tabletManager->GetTabletCellOrThrow(cellId);
        }

        tabletManager->PrepareMountTable(
            table,
            firstTabletIndex,
            lastTabletIndex,
            cell,
            freeze,
            mountTimestamp);
    }

    void HydraCommitMountTable(TTransaction* transaction, NTableClient::NProto::TReqMount* request)
    {
        int firstTabletIndex = request->first_tablet_index();
        int lastTabletIndex = request->last_tablet_index();
        auto cellId = FromProto<TTabletCellId>(request->cell_id());
        bool freeze = request->freeze();
        auto mountTimestamp = static_cast<TTimestamp>(request->mount_timestamp());
        auto tableId = FromProto<TTableId>(request->table_id());
        auto path = request->path();

        LOG_DEBUG_UNLESS(IsRecovery(), "Commit mount table (TableId: %v, TransactionId: %v, User: %v, "
            "FirstTabletIndex: %v, LastTabletIndex: %v, CellId: %v, Freeze: %v, MountTimestamp: %v)",
            tableId,
            transaction->GetId(),
            Bootstrap_->GetSecurityManager()->GetAuthenticatedUserName(),
            firstTabletIndex,
            lastTabletIndex,
            cellId,
            freeze,
            mountTimestamp);

        const auto& cypressManager = Bootstrap_->GetCypressManager();
        auto* table = cypressManager->FindNode(TVersionedNodeId(tableId))->As<TTableNode>();

        if (!IsObjectAlive(table)) {
            return;
        }

        table->SetLastMountTransactionId(transaction->GetId());
        table->UpdateExpectedTabletState(freeze ? ETabletState::Frozen : ETabletState::Mounted);

        if (table->IsExternal()) {
            return;
        }

        const auto& tabletManager = Bootstrap_->GetTabletManager();

        TTabletCell* cell = nullptr;
        if (cellId) {
            cell = tabletManager->FindTabletCell(cellId);
        }

        table->SetMountPath(path);

        tabletManager->MountTable(
            table,
            firstTabletIndex,
            lastTabletIndex,
            cell,
            freeze,
            mountTimestamp);
    }

    void HydraPrepareUnmountTable(TTransaction* transaction, NTableClient::NProto::TReqUnmount* request, bool persist)
    {
        int firstTabletIndex = request->first_tablet_index();
        int lastTabletIndex = request->last_tablet_index();
        bool force = request->force();
        auto tableId = FromProto<TTableId>(request->table_id());

        LOG_DEBUG_UNLESS(IsRecovery(), "Prepare unmount table (TableId: %v, TransactionId: %v, User: %v, "
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
        auto proxy = cypressManager->GetNodeProxy(table, transaction);
        proxy->CypressValidatePermission(EPermissionCheckScope::This, EPermission::Mount);

        cypressManager->LockNode(table, transaction, ELockMode::Exclusive, false, true);

        if (table->IsExternal()) {
            return;
        }

        const auto& tabletManager = Bootstrap_->GetTabletManager();

        tabletManager->PrepareUnmountTable(
            table,
            force,
            firstTabletIndex,
            lastTabletIndex); 
    }

    void HydraCommitUnmountTable(TTransaction* transaction, NTableClient::NProto::TReqUnmount* request)
    {
        int firstTabletIndex = request->first_tablet_index();
        int lastTabletIndex = request->last_tablet_index();
        bool force = request->force();
        auto tableId = FromProto<TTableId>(request->table_id());

        LOG_DEBUG_UNLESS(IsRecovery(), "Commit unmount table (TableId: %v, TransactionId: %v, User: %v, "
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

        if (table->IsExternal()) {
            return;
        }

        const auto& tabletManager = Bootstrap_->GetTabletManager();

        tabletManager->UnmountTable(
            table,
            force,
            firstTabletIndex,
            lastTabletIndex); 
    }

    void HydraPrepareFreezeTable(TTransaction* transaction, NTableClient::NProto::TReqFreeze* request, bool persist)
    {
        int firstTabletIndex = request->first_tablet_index();
        int lastTabletIndex = request->last_tablet_index();
        auto tableId = FromProto<TTableId>(request->table_id());

        LOG_DEBUG_UNLESS(IsRecovery(), "Prepare freeze table (TableId: %v, TransactionId: %v, User: %v, "
            "FirstTabletIndex: %v, LastTabletIndex: %v)",
            tableId,
            transaction->GetId(),
            Bootstrap_->GetSecurityManager()->GetAuthenticatedUserName(),
            firstTabletIndex,
            lastTabletIndex);

        ValidateNoParentTransaction(transaction);

        const auto& cypressManager = Bootstrap_->GetCypressManager();
        auto* table = cypressManager->GetNodeOrThrow(TVersionedNodeId(tableId))->As<TTableNode>();
        auto proxy = cypressManager->GetNodeProxy(table, transaction);
        proxy->CypressValidatePermission(EPermissionCheckScope::This, EPermission::Mount);

        cypressManager->LockNode(table, transaction, ELockMode::Exclusive, false, true);

        if (table->IsExternal()) {
            return;
        }

        const auto& tabletManager = Bootstrap_->GetTabletManager();

        tabletManager->PrepareFreezeTable(
            table,
            firstTabletIndex,
            lastTabletIndex); 
    }

    void HydraCommitFreezeTable(TTransaction* transaction, NTableClient::NProto::TReqFreeze* request)
    {
        int firstTabletIndex = request->first_tablet_index();
        int lastTabletIndex = request->last_tablet_index();
        auto tableId = FromProto<TTableId>(request->table_id());

        LOG_DEBUG_UNLESS(IsRecovery(), "Commit freeze table (TableId: %v, TransactionId: %v, User: %v, "
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

        if (table->IsExternal()) {
            return;
        }

        const auto& tabletManager = Bootstrap_->GetTabletManager();

        tabletManager->FreezeTable(
            table,
            firstTabletIndex,
            lastTabletIndex); 
    }

    void HydraPrepareUnfreezeTable(TTransaction* transaction, NTableClient::NProto::TReqUnfreeze* request, bool persist)
    {
        int firstTabletIndex = request->first_tablet_index();
        int lastTabletIndex = request->last_tablet_index();
        auto tableId = FromProto<TTableId>(request->table_id());

        LOG_DEBUG_UNLESS(IsRecovery(), "Prepare unfreeze table (TableId: %v, TransactionId: %v, User: %v, "
            "FirstTabletIndex: %v, LastTabletIndex: %v)",
            tableId,
            transaction->GetId(),
            Bootstrap_->GetSecurityManager()->GetAuthenticatedUserName(),
            firstTabletIndex,
            lastTabletIndex);

        ValidateNoParentTransaction(transaction);

        const auto& cypressManager = Bootstrap_->GetCypressManager();
        auto* table = cypressManager->GetNodeOrThrow(TVersionedNodeId(tableId))->As<TTableNode>();
        auto proxy = cypressManager->GetNodeProxy(table, transaction);
        proxy->CypressValidatePermission(EPermissionCheckScope::This, EPermission::Mount);

        cypressManager->LockNode(table, transaction, ELockMode::Exclusive, false, true);

        if (table->IsExternal()) {
            return;
        }

        const auto& tabletManager = Bootstrap_->GetTabletManager();

        tabletManager->PrepareUnfreezeTable(
            table,
            firstTabletIndex,
            lastTabletIndex); 
    }

    void HydraCommitUnfreezeTable(TTransaction* transaction, NTableClient::NProto::TReqUnfreeze* request)
    {
        int firstTabletIndex = request->first_tablet_index();
        int lastTabletIndex = request->last_tablet_index();
        auto tableId = FromProto<TTableId>(request->table_id());

        LOG_DEBUG_UNLESS(IsRecovery(), "Commit unfreeze table (TableId: %v, TransactionId: %v, User: %v, "
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

        if (table->IsExternal()) {
            return;
        }

        const auto& tabletManager = Bootstrap_->GetTabletManager();

        tabletManager->UnfreezeTable(
            table,
            firstTabletIndex,
            lastTabletIndex); 
    }

    void HydraPrepareRemountTable(TTransaction* transaction, NTableClient::NProto::TReqRemount* request, bool persist)
    {
        int firstTabletIndex = request->first_tablet_index();
        int lastTabletIndex = request->last_tablet_index();
        auto tableId = FromProto<TTableId>(request->table_id());

        LOG_DEBUG_UNLESS(IsRecovery(), "Prepare remount table (TableId: %v, TransactionId: %v, User: %v, "
            "FirstTabletIndex: %v, LastTabletIndex: %v)",
            tableId,
            transaction->GetId(),
            Bootstrap_->GetSecurityManager()->GetAuthenticatedUserName(),
            firstTabletIndex,
            lastTabletIndex);

        ValidateNoParentTransaction(transaction);

        const auto& cypressManager = Bootstrap_->GetCypressManager();
        auto* table = cypressManager->GetNodeOrThrow(TVersionedNodeId(tableId))->As<TTableNode>();
        auto proxy = cypressManager->GetNodeProxy(table, transaction);
        proxy->CypressValidatePermission(EPermissionCheckScope::This, EPermission::Mount);

        cypressManager->LockNode(table, transaction, ELockMode::Exclusive, false, true);

        if (table->IsExternal()) {
            return;
        }

        const auto& tabletManager = Bootstrap_->GetTabletManager();

        tabletManager->PrepareRemountTable(
            table,
            firstTabletIndex,
            lastTabletIndex); 
    }

    void HydraCommitRemountTable(TTransaction* transaction, NTableClient::NProto::TReqRemount* request)
    {
        int firstTabletIndex = request->first_tablet_index();
        int lastTabletIndex = request->last_tablet_index();
        auto tableId = FromProto<TTableId>(request->table_id());

        LOG_DEBUG_UNLESS(IsRecovery(), "Commit remount table (TableId: %v, TransactionId: %v, User: %v, "
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

        auto proxy = cypressManager->GetNodeProxy(table, transaction);
        proxy->CypressValidatePermission(EPermissionCheckScope::This, EPermission::Mount);

        if (table->IsExternal()) {
            return;
        }

        const auto& tabletManager = Bootstrap_->GetTabletManager();

        tabletManager->RemountTable(
            table,
            firstTabletIndex,
            lastTabletIndex); 
    }

    void HydraPrepareReshardTable(TTransaction* transaction, NTableClient::NProto::TReqReshard* request, bool persist)
    {
        int firstTabletIndex = request->first_tablet_index();
        int lastTabletIndex = request->last_tablet_index();
        int tabletCount = request->tablet_count();
        auto pivotKeys = FromProto<std::vector<TOwningKey>>(request->pivot_keys());
        auto tableId = FromProto<TTableId>(request->table_id());

        LOG_DEBUG_UNLESS(IsRecovery(), "Prepare reshard table (TableId: %v, TransactionId: %v, User: %v, "
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
        auto proxy = cypressManager->GetNodeProxy(table, transaction);
        proxy->CypressValidatePermission(EPermissionCheckScope::This, EPermission::Mount);

        cypressManager->LockNode(table, transaction, ELockMode::Exclusive, false, true);

        if (table->IsExternal()) {
            return;
        }

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

    void HydraCommitReshardTable(TTransaction* transaction, NTableClient::NProto::TReqReshard* request)
    {
        int firstTabletIndex = request->first_tablet_index();
        int lastTabletIndex = request->last_tablet_index();
        int tabletCount = request->tablet_count();
        auto pivotKeys = FromProto<std::vector<TOwningKey>>(request->pivot_keys());
        auto tableId = FromProto<TTableId>(request->table_id());

        LOG_DEBUG_UNLESS(IsRecovery(), "Commit reshard table (TableId: %v, TransactionId: %v, User: %v, "
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

        auto proxy = cypressManager->GetNodeProxy(table, transaction);
        proxy->CypressValidatePermission(EPermissionCheckScope::This, EPermission::Mount);

        table->SetLastMountTransactionId(transaction->GetId());

        if (table->IsExternal()) {
            return;
        }

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

} // namespace NTabletServer
} // namespace NYT

