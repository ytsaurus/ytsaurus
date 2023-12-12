#include "private.h"
#include "tablet_manager.h"
#include "tablet_service.h"

#include <yt/yt/server/master/cell_master/config.h>
#include <yt/yt/server/master/cell_master/config_manager.h>
#include <yt/yt/server/master/cell_master/bootstrap.h>
#include <yt/yt/server/master/cell_master/hydra_facade.h>

#include <yt/yt/server/master/security_server/security_manager.h>
#include <yt/yt/server/master/security_server/access_log.h>

#include <yt/yt/server/lib/transaction_supervisor/helpers.h>

#include <yt/yt/ytlib/tablet_client/master_tablet_service.h>

#include <yt/yt/core/rpc/authentication_identity.h>

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
using namespace NTransactionSupervisor;
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
            MakeTransactionActionHandlerDescriptor(BIND(&TImpl::HydraPrepareMount, Unretained(this))),
            MakeTransactionActionHandlerDescriptor(BIND(&TImpl::HydraCommitMount, Unretained(this))),
            MakeTransactionActionHandlerDescriptor(BIND(&TImpl::HydraAbortMount, Unretained(this))));

        transactionManager->RegisterTransactionActionHandlers(
            MakeTransactionActionHandlerDescriptor(BIND(&TImpl::HydraPrepareUnmount, Unretained(this))),
            MakeTransactionActionHandlerDescriptor(BIND(&TImpl::HydraCommitUnmount, Unretained(this))),
            MakeTransactionActionHandlerDescriptor(BIND(&TImpl::HydraAbortUnmount, Unretained(this))));

        transactionManager->RegisterTransactionActionHandlers(
            MakeTransactionActionHandlerDescriptor(BIND(&TImpl::HydraPrepareFreeze, Unretained(this))),
            MakeTransactionActionHandlerDescriptor(BIND(&TImpl::HydraCommitFreeze, Unretained(this))),
            MakeTransactionActionHandlerDescriptor(BIND(&TImpl::HydraAbortFreeze, Unretained(this))));

        transactionManager->RegisterTransactionActionHandlers(
            MakeTransactionActionHandlerDescriptor(BIND(&TImpl::HydraPrepareUnfreeze, Unretained(this))),
            MakeTransactionActionHandlerDescriptor(BIND(&TImpl::HydraCommitUnfreeze, Unretained(this))),
            MakeTransactionActionHandlerDescriptor(BIND(&TImpl::HydraAbortUnfreeze, Unretained(this))));

        transactionManager->RegisterTransactionActionHandlers(
            MakeTransactionActionHandlerDescriptor(BIND(&TImpl::HydraPrepareRemount, Unretained(this))),
            MakeTransactionActionHandlerDescriptor(BIND(&TImpl::HydraCommitRemount, Unretained(this))),
            MakeTransactionActionHandlerDescriptor(BIND(&TImpl::HydraAbortRemount, Unretained(this))));

        transactionManager->RegisterTransactionActionHandlers(
            MakeTransactionActionHandlerDescriptor(BIND(&TImpl::HydraPrepareReshard, Unretained(this))),
            MakeTransactionActionHandlerDescriptor(BIND(&TImpl::HydraCommitReshard, Unretained(this))),
            MakeTransactionActionHandlerDescriptor(BIND(&TImpl::HydraAbortReshard, Unretained(this))));
    }

private:
    DECLARE_THREAD_AFFINITY_SLOT(AutomatonThread);


    static void ValidateNoParentTransaction(TTransaction* transaction)
    {
        if (transaction->GetParent()) {
            THROW_ERROR_EXCEPTION("Operation cannot be performed in transaction");
        }
    }

    static TTabletOwnerBase* AsTabletOwnerSafe(TCypressNode* node)
    {
        if (!node) {
            return nullptr;
        }
        if (!IsTabletOwnerType(node->GetType())) {
            THROW_ERROR_EXCEPTION("%v is not a tablet owner", node->GetId());
        }
        return node->As<TTabletOwnerBase>();
    }


    void ValidateUsePermissionOnCellBundle(TTabletOwnerBase* table)
    {
        const auto& securityManager = Bootstrap_->GetSecurityManager();
        const auto& cellBundle = table->TabletCellBundle();
        securityManager->ValidatePermission(cellBundle.Get(), EPermission::Use);
    }


    void HydraPrepareMount(
        TTransaction* transaction,
        NTabletClient::NProto::TReqMount* request,
        const TTransactionPrepareOptions& /*options*/)
    {
        int firstTabletIndex = request->first_tablet_index();
        int lastTabletIndex = request->last_tablet_index();
        auto hintCellId = FromProto<TTabletCellId>(request->cell_id());
        bool freeze = request->freeze();
        auto mountTimestamp = static_cast<TTimestamp>(request->mount_timestamp());
        auto tableId = FromProto<TTableId>(request->table_id());
        const auto& path = request->path();
        auto targetCellIds = FromProto<std::vector<TTabletCellId>>(request->target_cell_ids());

        const auto& securityManager = Bootstrap_->GetSecurityManager();
        TAuthenticatedUserGuard userGuard(securityManager);

        YT_LOG_DEBUG("Preparing table mount (TableId: %v, TransactionId: %v, %v, "
            "FirstTabletIndex: %v, LastTabletIndex: %v, CellId: %v, TargetCellIds: %v, Freeze: %v, MountTimestamp: %v)",
            tableId,
            transaction->GetId(),
            NRpc::GetCurrentAuthenticationIdentity(),
            firstTabletIndex,
            lastTabletIndex,
            hintCellId,
            targetCellIds,
            freeze,
            mountTimestamp);

        ValidateNoParentTransaction(transaction);

        const auto& cypressManager = Bootstrap_->GetCypressManager();
        auto* table = AsTabletOwnerSafe(cypressManager->GetNodeOrThrow(TVersionedNodeId(tableId)));

        table->ValidateNoCurrentMountTransaction(Format("Cannot mount %v", table->GetLowercaseObjectName()));

        if (table->IsNative()) {
            auto currentPath = cypressManager->GetNodePath(table, nullptr);
            if (path != currentPath) {
                THROW_ERROR_EXCEPTION("%v path mismatch", table->GetCapitalizedObjectName())
                    << TErrorAttribute("requested_path", path)
                    << TErrorAttribute("resolved_path", currentPath);
            }

            ValidateUsePermissionOnCellBundle(table);

            cypressManager->LockNode(table, transaction, ELockMode::Exclusive, false, true);
        }

        const auto& tabletManager = Bootstrap_->GetTabletManager();
        tabletManager->PrepareMount(
            table,
            firstTabletIndex,
            lastTabletIndex,
            hintCellId,
            targetCellIds,
            freeze);

        // CurrentMountTransactionId is used to prevent primary master to copy/move node when
        // secondary master has already committed mount (this causes an unexpected error in CloneTable).
        // Primary master is lazy coordinator of 2pc, thus clone command and participant commit command are
        // serialized. Moreover secondary master (participant) commit happens strictly before primary commit.
        // CurrentMountTransactionId mechanism ensures that clone command can be sent only before
        // primary master has been started participating in 2pc. Thus clone command cannot appear
        // on the secondary master after commit. It can however arrive between prepare and commit
        // so we don't call this validation on secondary master. Note that this deals with
        // clone command 'before' mount. Refer to UpdateTabletState to see how we deal with it 'after' mount.
        //
        // We also lock node on secondary master to prevent resharding tablet actions to change table structure
        // during two phase mount.
        table->LockCurrentMountTransaction(transaction->GetId());

        YT_LOG_ACCESS(tableId, request->path(), transaction, "PrepareMount");
    }

    void HydraCommitMount(
        TTransaction* transaction,
        NTabletClient::NProto::TReqMount* request,
        const TTransactionCommitOptions& /*options*/)
    {
        int firstTabletIndex = request->first_tablet_index();
        int lastTabletIndex = request->last_tablet_index();
        auto hintCellId = FromProto<TTabletCellId>(request->cell_id());
        bool freeze = request->freeze();
        auto mountTimestamp = static_cast<TTimestamp>(request->mount_timestamp());
        auto tableId = FromProto<TTableId>(request->table_id());
        const auto& path = request->path();
        auto targetCellIds = FromProto<std::vector<TTabletCellId>>(request->target_cell_ids());

        YT_LOG_DEBUG("Committing table mount (TableId: %v, TransactionId: %v, %v, "
            "FirstTabletIndex: %v, LastTabletIndex: %v, CellId: %v, TargetCellIds: %v, Freeze: %v, MountTimestamp: %v)",
            tableId,
            transaction->GetId(),
            NRpc::GetCurrentAuthenticationIdentity(),
            firstTabletIndex,
            lastTabletIndex,
            hintCellId,
            targetCellIds,
            freeze,
            mountTimestamp);

        const auto& cypressManager = Bootstrap_->GetCypressManager();
        auto* table = AsTabletOwnerSafe(cypressManager->FindNode(TVersionedNodeId(tableId)));

        if (!IsObjectAlive(table)) {
            return;
        }

        table->UnlockCurrentMountTransaction(transaction->GetId());

        table->SetLastMountTransactionId(transaction->GetId());
        table->UpdateExpectedTabletState(freeze ? ETabletState::Frozen : ETabletState::Mounted);

        const auto& tabletManager = Bootstrap_->GetTabletManager();
        tabletManager->Mount(
            table,
            path,
            firstTabletIndex,
            lastTabletIndex,
            hintCellId,
            targetCellIds,
            freeze,
            mountTimestamp);

        YT_LOG_ACCESS(tableId, request->path(), transaction, "CommitMount");
    }

    void HydraAbortMount(
        TTransaction* transaction,
        NTabletClient::NProto::TReqMount* request,
        const TTransactionAbortOptions& /*options*/)
    {
        int firstTabletIndex = request->first_tablet_index();
        int lastTabletIndex = request->last_tablet_index();
        auto hintCellId = FromProto<TTabletCellId>(request->cell_id());
        bool freeze = request->freeze();
        auto mountTimestamp = static_cast<TTimestamp>(request->mount_timestamp());
        auto tableId = FromProto<TTableId>(request->table_id());
        auto targetCellIds = FromProto<std::vector<TTabletCellId>>(request->target_cell_ids());

        YT_LOG_DEBUG("Aborting table mount (TableId: %v, TransactionId: %v, %v, "
            "FirstTabletIndex: %v, LastTabletIndex: %v, CellId: %v, TargetCellIds: %v, Freeze: %v, MountTimestamp: %v)",
            tableId,
            transaction->GetId(),
            NRpc::GetCurrentAuthenticationIdentity(),
            firstTabletIndex,
            lastTabletIndex,
            hintCellId,
            targetCellIds,
            freeze,
            mountTimestamp);

        const auto& cypressManager = Bootstrap_->GetCypressManager();
        auto* table = AsTabletOwnerSafe(cypressManager->FindNode(TVersionedNodeId(tableId)));

        if (!IsObjectAlive(table)) {
            return;
        }

        table->UnlockCurrentMountTransaction(transaction->GetId());

        YT_LOG_ACCESS(tableId, request->path(), transaction, "AbortMount");
    }

    void HydraPrepareUnmount(
        TTransaction* transaction,
        NTabletClient::NProto::TReqUnmount* request,
        const TTransactionPrepareOptions& /*options*/)
    {
        int firstTabletIndex = request->first_tablet_index();
        int lastTabletIndex = request->last_tablet_index();
        bool force = request->force();
        auto tableId = FromProto<TTableId>(request->table_id());

        const auto& securityManager = Bootstrap_->GetSecurityManager();
        TAuthenticatedUserGuard userGuard(securityManager);

        YT_LOG_DEBUG("Preparing table unmount (TableId: %v, TransactionId: %v, %v, "
            "Force: %v, FirstTabletIndex: %v, LastTabletIndex: %v)",
            tableId,
            transaction->GetId(),
            NRpc::GetCurrentAuthenticationIdentity(),
            force,
            firstTabletIndex,
            lastTabletIndex);

        ValidateNoParentTransaction(transaction);

        const auto& cypressManager = Bootstrap_->GetCypressManager();
        auto* table = AsTabletOwnerSafe(cypressManager->GetNodeOrThrow(TVersionedNodeId(tableId)));

        ValidateUsePermissionOnCellBundle(table);

        if (force) {
            const auto& cellBundle = table->TabletCellBundle();
            securityManager->ValidatePermission(cellBundle.Get(), EPermission::Administer);
        }

        table->ValidateNoCurrentMountTransaction(Format("Cannot unmount %v", table->GetLowercaseObjectName()));

        if (table->IsNative()) {
            cypressManager->LockNode(table, transaction, ELockMode::Exclusive, false, true);
        }

        const auto& tabletManager = Bootstrap_->GetTabletManager();
        tabletManager->PrepareUnmount(
            table,
            force,
            firstTabletIndex,
            lastTabletIndex);

        table->LockCurrentMountTransaction(transaction->GetId());

        YT_LOG_ACCESS(tableId, cypressManager->GetNodePath(table, nullptr), transaction, "PrepareUnmount");
    }

    void HydraCommitUnmount(
        TTransaction* transaction,
        NTabletClient::NProto::TReqUnmount* request,
        const TTransactionCommitOptions& /*options*/)
    {
        int firstTabletIndex = request->first_tablet_index();
        int lastTabletIndex = request->last_tablet_index();
        bool force = request->force();
        auto tableId = FromProto<TTableId>(request->table_id());

        YT_LOG_DEBUG("Committing table unmount (TableId: %v, TransactionId: %v, %v, "
            "Force: %v, FirstTabletIndex: %v, LastTabletIndex: %v)",
            tableId,
            transaction->GetId(),
            NRpc::GetCurrentAuthenticationIdentity(),
            force,
            firstTabletIndex,
            lastTabletIndex);

        const auto& cypressManager = Bootstrap_->GetCypressManager();
        auto* table = AsTabletOwnerSafe(cypressManager->FindNode(TVersionedNodeId(tableId)));

        if (!IsObjectAlive(table)) {
            return;
        }

        table->UnlockCurrentMountTransaction(transaction->GetId());

        table->SetLastMountTransactionId(transaction->GetId());

        const auto& tabletManager = Bootstrap_->GetTabletManager();
        tabletManager->Unmount(
            table,
            force,
            firstTabletIndex,
            lastTabletIndex);

        YT_LOG_ACCESS(tableId, cypressManager->GetNodePath(table, nullptr), transaction, "CommitUnmount");
    }

    void HydraAbortUnmount(
        TTransaction* transaction,
        NTabletClient::NProto::TReqUnmount* request,
        const TTransactionAbortOptions& /*options*/)
    {
        int firstTabletIndex = request->first_tablet_index();
        int lastTabletIndex = request->last_tablet_index();
        bool force = request->force();
        auto tableId = FromProto<TTableId>(request->table_id());

        YT_LOG_DEBUG("Aborting table unmount (TableId: %v, TransactionId: %v, %v, "
            "Force: %v, FirstTabletIndex: %v, LastTabletIndex: %v)",
            tableId,
            transaction->GetId(),
            NRpc::GetCurrentAuthenticationIdentity(),
            force,
            firstTabletIndex,
            lastTabletIndex);

        const auto& cypressManager = Bootstrap_->GetCypressManager();
        auto* table = AsTabletOwnerSafe(cypressManager->FindNode(TVersionedNodeId(tableId)));

        if (!IsObjectAlive(table)) {
            return;
        }

        table->UnlockCurrentMountTransaction(transaction->GetId());

        YT_LOG_ACCESS(tableId, cypressManager->GetNodePath(table, nullptr), transaction, "AbortUnmount");
    }

    void HydraPrepareFreeze(
        TTransaction* transaction,
        NTabletClient::NProto::TReqFreeze* request,
        const TTransactionPrepareOptions& /*options*/)
    {
        int firstTabletIndex = request->first_tablet_index();
        int lastTabletIndex = request->last_tablet_index();
        auto tableId = FromProto<TTableId>(request->table_id());

        const auto& securityManager = Bootstrap_->GetSecurityManager();
        TAuthenticatedUserGuard userGuard(securityManager);

        YT_LOG_DEBUG("Preparing table freeze (TableId: %v, TransactionId: %v, %v, "
            "FirstTabletIndex: %v, LastTabletIndex: %v)",
            tableId,
            transaction->GetId(),
            NRpc::GetCurrentAuthenticationIdentity(),
            firstTabletIndex,
            lastTabletIndex);

        ValidateNoParentTransaction(transaction);

        const auto& cypressManager = Bootstrap_->GetCypressManager();
        auto* table = AsTabletOwnerSafe(cypressManager->GetNodeOrThrow(TVersionedNodeId(tableId)));

        ValidateUsePermissionOnCellBundle(table);

        table->ValidateNoCurrentMountTransaction(Format("Cannot freeze %v", table->GetLowercaseObjectName()));

        if (table->IsNative()) {
            cypressManager->LockNode(table, transaction, ELockMode::Exclusive, false, true);
        }

        const auto& tabletManager = Bootstrap_->GetTabletManager();
        tabletManager->PrepareFreeze(
            table,
            firstTabletIndex,
            lastTabletIndex);

        table->LockCurrentMountTransaction(transaction->GetId());

        YT_LOG_ACCESS(tableId, cypressManager->GetNodePath(table, nullptr), transaction, "PrepareFreeze");
    }

    void HydraCommitFreeze(
        TTransaction* transaction,
        NTabletClient::NProto::TReqFreeze* request,
        const TTransactionCommitOptions& /*options*/)
    {
        int firstTabletIndex = request->first_tablet_index();
        int lastTabletIndex = request->last_tablet_index();
        auto tableId = FromProto<TTableId>(request->table_id());

        YT_LOG_DEBUG("Committing table freeze (TableId: %v, TransactionId: %v, %v, "
            "FirstTabletIndex: %v, LastTabletIndex: %v)",
            tableId,
            transaction->GetId(),
            NRpc::GetCurrentAuthenticationIdentity(),
            firstTabletIndex,
            lastTabletIndex);

        const auto& cypressManager = Bootstrap_->GetCypressManager();
        auto* table = AsTabletOwnerSafe(cypressManager->FindNode(TVersionedNodeId(tableId)));

        if (!IsObjectAlive(table)) {
            return;
        }

        table->UnlockCurrentMountTransaction(transaction->GetId());

        table->SetLastMountTransactionId(transaction->GetId());

        const auto& tabletManager = Bootstrap_->GetTabletManager();
        tabletManager->Freeze(
            table,
            firstTabletIndex,
            lastTabletIndex);

        YT_LOG_ACCESS(tableId, cypressManager->GetNodePath(table, nullptr), transaction, "CommitFreeze");
    }

    void HydraAbortFreeze(
        TTransaction* transaction,
        NTabletClient::NProto::TReqFreeze* request,
        const TTransactionAbortOptions& /*options*/)
    {
        int firstTabletIndex = request->first_tablet_index();
        int lastTabletIndex = request->last_tablet_index();
        auto tableId = FromProto<TTableId>(request->table_id());

        YT_LOG_DEBUG("Aborting table freeze (TableId: %v, TransactionId: %v, %v, "
            "FirstTabletIndex: %v, LastTabletIndex: %v)",
            tableId,
            transaction->GetId(),
            NRpc::GetCurrentAuthenticationIdentity(),
            firstTabletIndex,
            lastTabletIndex);

        const auto& cypressManager = Bootstrap_->GetCypressManager();
        auto* table = AsTabletOwnerSafe(cypressManager->FindNode(TVersionedNodeId(tableId)));

        if (!IsObjectAlive(table)) {
            return;
        }

        table->UnlockCurrentMountTransaction(transaction->GetId());

        YT_LOG_ACCESS(tableId, cypressManager->GetNodePath(table, nullptr), transaction, "AbortFreeze");
    }

    void HydraPrepareUnfreeze(
        TTransaction* transaction,
        NTabletClient::NProto::TReqUnfreeze* request,
        const TTransactionPrepareOptions& /*options*/)
    {
        int firstTabletIndex = request->first_tablet_index();
        int lastTabletIndex = request->last_tablet_index();
        auto tableId = FromProto<TTableId>(request->table_id());

        const auto& securityManager = Bootstrap_->GetSecurityManager();
        TAuthenticatedUserGuard userGuard(securityManager);

        YT_LOG_DEBUG("Preparing table unfreeze (TableId: %v, TransactionId: %v, %v, "
            "FirstTabletIndex: %v, LastTabletIndex: %v)",
            tableId,
            transaction->GetId(),
            NRpc::GetCurrentAuthenticationIdentity(),
            firstTabletIndex,
            lastTabletIndex);

        ValidateNoParentTransaction(transaction);

        const auto& cypressManager = Bootstrap_->GetCypressManager();
        auto* table = AsTabletOwnerSafe(cypressManager->GetNodeOrThrow(TVersionedNodeId(tableId)));

        ValidateUsePermissionOnCellBundle(table);

        table->ValidateNoCurrentMountTransaction(Format("Cannot unfreeze %v", table->GetLowercaseObjectName()));

        if (table->IsNative()) {
            cypressManager->LockNode(table, transaction, ELockMode::Exclusive, false, true);
        }

        const auto& tabletManager = Bootstrap_->GetTabletManager();
        tabletManager->PrepareUnfreeze(
            table,
            firstTabletIndex,
            lastTabletIndex);

        table->LockCurrentMountTransaction(transaction->GetId());

        YT_LOG_ACCESS(tableId, cypressManager->GetNodePath(table, nullptr), transaction, "PrepareUnfreeze");
    }

    void HydraCommitUnfreeze(
        TTransaction* transaction,
        NTabletClient::NProto::TReqUnfreeze* request,
        const TTransactionCommitOptions& /*options*/)
    {
        int firstTabletIndex = request->first_tablet_index();
        int lastTabletIndex = request->last_tablet_index();
        auto tableId = FromProto<TTableId>(request->table_id());

        YT_LOG_DEBUG("Committing table unfreeze (TableId: %v, TransactionId: %v, %v, "
            "FirstTabletIndex: %v, LastTabletIndex: %v)",
            tableId,
            transaction->GetId(),
            NRpc::GetCurrentAuthenticationIdentity(),
            firstTabletIndex,
            lastTabletIndex);

        const auto& cypressManager = Bootstrap_->GetCypressManager();
        auto* table = AsTabletOwnerSafe(cypressManager->FindNode(TVersionedNodeId(tableId)));

        if (!IsObjectAlive(table)) {
            return;
        }

        table->UnlockCurrentMountTransaction(transaction->GetId());

        table->SetLastMountTransactionId(transaction->GetId());
        table->UpdateExpectedTabletState(ETabletState::Mounted);

        const auto& tabletManager = Bootstrap_->GetTabletManager();
        tabletManager->Unfreeze(
            table,
            firstTabletIndex,
            lastTabletIndex);

        YT_LOG_ACCESS(tableId, cypressManager->GetNodePath(table, nullptr), transaction, "CommitUnfreeze");
    }

    void HydraAbortUnfreeze(
        TTransaction* transaction,
        NTabletClient::NProto::TReqUnfreeze* request,
        const TTransactionAbortOptions& /*options*/)
    {
        int firstTabletIndex = request->first_tablet_index();
        int lastTabletIndex = request->last_tablet_index();
        auto tableId = FromProto<TTableId>(request->table_id());

        YT_LOG_DEBUG("Aborting table unfreeze (TableId: %v, TransactionId: %v, %v, "
            "FirstTabletIndex: %v, LastTabletIndex: %v)",
            tableId,
            transaction->GetId(),
            NRpc::GetCurrentAuthenticationIdentity(),
            firstTabletIndex,
            lastTabletIndex);

        const auto& cypressManager = Bootstrap_->GetCypressManager();
        auto* table = AsTabletOwnerSafe(cypressManager->FindNode(TVersionedNodeId(tableId)));

        if (!IsObjectAlive(table)) {
            return;
        }

        table->UnlockCurrentMountTransaction(transaction->GetId());

        YT_LOG_ACCESS(tableId, cypressManager->GetNodePath(table, nullptr), transaction, "AbortUnfreeze");
    }

    void HydraPrepareRemount(
        TTransaction* transaction,
        NTabletClient::NProto::TReqRemount* request,
        const TTransactionPrepareOptions& /*options*/)
    {
        int firstTabletIndex = request->first_tablet_index();
        int lastTabletIndex = request->last_tablet_index();
        auto tableId = FromProto<TTableId>(request->table_id());

        const auto& securityManager = Bootstrap_->GetSecurityManager();
        TAuthenticatedUserGuard userGuard(securityManager);

        YT_LOG_DEBUG("Preparing table remount (TableId: %v, TransactionId: %v, %v, "
            "FirstTabletIndex: %v, LastTabletIndex: %v)",
            tableId,
            transaction->GetId(),
            NRpc::GetCurrentAuthenticationIdentity(),
            firstTabletIndex,
            lastTabletIndex);

        ValidateNoParentTransaction(transaction);

        const auto& cypressManager = Bootstrap_->GetCypressManager();
        auto* table = AsTabletOwnerSafe(cypressManager->GetNodeOrThrow(TVersionedNodeId(tableId)));

        ValidateUsePermissionOnCellBundle(table);

        table->ValidateNoCurrentMountTransaction(Format("Cannot remount %v", table->GetLowercaseObjectName()));

        if (table->IsNative()) {
            cypressManager->LockNode(table, transaction, ELockMode::Exclusive, false, true);
        }

        const auto& tabletManager = Bootstrap_->GetTabletManager();
        tabletManager->PrepareRemount(
            table,
            firstTabletIndex,
            lastTabletIndex);

        table->LockCurrentMountTransaction(transaction->GetId());

        YT_LOG_ACCESS(tableId, cypressManager->GetNodePath(table, nullptr), transaction, "PrepareRemount");
    }

    void HydraCommitRemount(
        TTransaction* transaction,
        NTabletClient::NProto::TReqRemount* request,
        const TTransactionCommitOptions& /*options*/)
    {
        int firstTabletIndex = request->first_tablet_index();
        int lastTabletIndex = request->last_tablet_index();
        auto tableId = FromProto<TTableId>(request->table_id());

        YT_LOG_DEBUG("Committing table remount (TableId: %v, TransactionId: %v, %v, "
            "FirstTabletIndex: %v, LastTabletIndex: %v)",
            tableId,
            transaction->GetId(),
            NRpc::GetCurrentAuthenticationIdentity(),
            firstTabletIndex,
            lastTabletIndex);

        const auto& cypressManager = Bootstrap_->GetCypressManager();
        auto* table = AsTabletOwnerSafe(cypressManager->FindNode(TVersionedNodeId(tableId)));

        if (!IsObjectAlive(table)) {
            return;
        }

        table->UnlockCurrentMountTransaction(transaction->GetId());

        const auto& tabletManager = Bootstrap_->GetTabletManager();
        tabletManager->Remount(
            table,
            firstTabletIndex,
            lastTabletIndex);

        YT_LOG_ACCESS(tableId, cypressManager->GetNodePath(table, nullptr), transaction, "CommitRemount");
    }

    void HydraAbortRemount(
        TTransaction* transaction,
        NTabletClient::NProto::TReqRemount* request,
        const TTransactionAbortOptions& /*options*/)
    {
        int firstTabletIndex = request->first_tablet_index();
        int lastTabletIndex = request->last_tablet_index();
        auto tableId = FromProto<TTableId>(request->table_id());

        YT_LOG_DEBUG("Aborting table remount (TableId: %v, TransactionId: %v, %v, "
            "FirstTabletIndex: %v, LastTabletIndex: %v)",
            tableId,
            transaction->GetId(),
            NRpc::GetCurrentAuthenticationIdentity(),
            firstTabletIndex,
            lastTabletIndex);

        const auto& cypressManager = Bootstrap_->GetCypressManager();
        auto* table = AsTabletOwnerSafe(cypressManager->FindNode(TVersionedNodeId(tableId)));

        if (!IsObjectAlive(table)) {
            return;
        }

        table->UnlockCurrentMountTransaction(transaction->GetId());

        YT_LOG_ACCESS(tableId, cypressManager->GetNodePath(table, nullptr), transaction, "AbortRemount");
    }

    void HydraPrepareReshard(
        TTransaction* transaction,
        NTabletClient::NProto::TReqReshard* request,
        const TTransactionPrepareOptions& /*options*/)
    {
        int firstTabletIndex = request->first_tablet_index();
        int lastTabletIndex = request->last_tablet_index();
        int tabletCount = request->tablet_count();
        auto pivotKeys = FromProto<std::vector<TLegacyOwningKey>>(request->pivot_keys());
        auto tableId = FromProto<TTableId>(request->table_id());
        auto trimmedRowCounts = FromProto<std::vector<i64>>(request->trimmed_row_counts());

        const auto& securityManager = Bootstrap_->GetSecurityManager();
        TAuthenticatedUserGuard userGuard(securityManager);

        YT_LOG_DEBUG("Preparing table reshard (TableId: %v, TransactionId: %v, %v, "
            "TabletCount: %v, PivotKeysSize: %v, TrimmedRowCountsSize: %v, "
            "FirstTabletIndex: %v, LastTabletIndex: %v)",
            tableId,
            transaction->GetId(),
            NRpc::GetCurrentAuthenticationIdentity(),
            tabletCount,
            pivotKeys.size(),
            trimmedRowCounts.size(),
            firstTabletIndex,
            lastTabletIndex);

        ValidateNoParentTransaction(transaction);

        const auto& cypressManager = Bootstrap_->GetCypressManager();
        auto* table = AsTabletOwnerSafe(cypressManager->FindNode(TVersionedNodeId(tableId)));

        ValidateUsePermissionOnCellBundle(table);

        table->ValidateNoCurrentMountTransaction(Format("Cannot reshard %v", table->GetLowercaseObjectName()));

        if (table->IsNative()) {
            cypressManager->LockNode(table, transaction, ELockMode::Exclusive, false, true);
        }

        const auto& tabletManager = Bootstrap_->GetTabletManager();
        tabletManager->PrepareReshard(
            table,
            firstTabletIndex,
            lastTabletIndex,
            tabletCount,
            pivotKeys,
            trimmedRowCounts);

        table->LockCurrentMountTransaction(transaction->GetId());

        YT_LOG_ACCESS(tableId, cypressManager->GetNodePath(table, nullptr), transaction, "PrepareReshard");
    }

    void HydraCommitReshard(
        TTransaction* transaction,
        NTabletClient::NProto::TReqReshard* request,
        const TTransactionCommitOptions& /*options*/)
    {
        int firstTabletIndex = request->first_tablet_index();
        int lastTabletIndex = request->last_tablet_index();
        int tabletCount = request->tablet_count();
        auto pivotKeys = FromProto<std::vector<TLegacyOwningKey>>(request->pivot_keys());
        auto tableId = FromProto<TTableId>(request->table_id());
        auto trimmedRowCounts = FromProto<std::vector<i64>>(request->trimmed_row_counts());

        YT_LOG_DEBUG("Committing table reshard (TableId: %v, TransactionId: %v, %v, "
            "TabletCount: %v, PivotKeysSize: %v, TrimmedRowCountsSize: %v, "
            "FirstTabletIndex: %v, LastTabletIndex: %v)",
            tableId,
            transaction->GetId(),
            NRpc::GetCurrentAuthenticationIdentity(),
            tabletCount,
            pivotKeys.size(),
            trimmedRowCounts.size(),
            firstTabletIndex,
            lastTabletIndex);

        const auto& cypressManager = Bootstrap_->GetCypressManager();
        auto* table = AsTabletOwnerSafe(cypressManager->FindNode(TVersionedNodeId(tableId)));

        if (!IsObjectAlive(table)) {
            return;
        }

        table->UnlockCurrentMountTransaction(transaction->GetId());

        table->SetLastMountTransactionId(transaction->GetId());

        const auto& tabletManager = Bootstrap_->GetTabletManager();
        tabletManager->Reshard(
            table,
            firstTabletIndex,
            lastTabletIndex,
            tabletCount,
            pivotKeys,
            trimmedRowCounts);

        YT_LOG_ACCESS(tableId, cypressManager->GetNodePath(table, nullptr), transaction, "CommitReshard");
    }

    void HydraAbortReshard(
        TTransaction* transaction,
        NTabletClient::NProto::TReqReshard* request,
        const TTransactionAbortOptions& /*options*/)
    {
        int firstTabletIndex = request->first_tablet_index();
        int lastTabletIndex = request->last_tablet_index();
        int tabletCount = request->tablet_count();
        auto pivotKeys = FromProto<std::vector<TLegacyOwningKey>>(request->pivot_keys());
        auto tableId = FromProto<TTableId>(request->table_id());

        YT_LOG_DEBUG("Aborting table reshard (TableId: %v, TransactionId: %v, %v, "
            "TabletCount: %v, PivotKeysSize: %v, FirstTabletIndex: %v, LastTabletIndex: %v)",
            tableId,
            transaction->GetId(),
            NRpc::GetCurrentAuthenticationIdentity(),
            tabletCount,
            pivotKeys.size(),
            firstTabletIndex,
            lastTabletIndex);

        const auto& cypressManager = Bootstrap_->GetCypressManager();
        auto* table = AsTabletOwnerSafe(cypressManager->FindNode(TVersionedNodeId(tableId)));

        if (!IsObjectAlive(table)) {
            return;
        }

        table->UnlockCurrentMountTransaction(transaction->GetId());

        YT_LOG_ACCESS(tableId, cypressManager->GetNodePath(table, nullptr), transaction, "AbortReshard");
    }
};

////////////////////////////////////////////////////////////////////////////////

TTabletService::TTabletService(
    NCellMaster::TBootstrap* bootstrap)
    : Impl_(New<TImpl>(bootstrap))
{ }

TTabletService::~TTabletService()
{ }

void TTabletService::Initialize()
{
    Impl_->Initialize();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletServer
