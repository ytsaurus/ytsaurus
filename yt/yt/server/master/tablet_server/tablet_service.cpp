#include "private.h"
#include "tablet_cell_bundle.h"
#include "tablet_manager.h"
#include "tablet_owner_base.h"
#include "tablet_service.h"

#include <yt/yt/server/master/cell_master/config.h>
#include <yt/yt/server/master/cell_master/config_manager.h>
#include <yt/yt/server/master/cell_master/bootstrap.h>
#include <yt/yt/server/master/cell_master/hydra_facade.h>

#include <yt/yt/server/master/cypress_server/cypress_manager.h>

#include <yt/yt/server/master/security_server/security_manager.h>
#include <yt/yt/server/master/security_server/access_log.h>

#include <yt/yt/server/master/table_server/table_node.h>

#include <yt/yt/ytlib/tablet_client/master_tablet_service.h>

#include <yt/yt/core/rpc/authentication_identity.h>

namespace NYT::NTabletServer {

using namespace NCellMaster;
using namespace NConcurrency;
using namespace NCypressClient;
using namespace NCypressServer;
using namespace NHiveServer;
using namespace NHydra;
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

////////////////////////////////////////////////////////////////////////////////

constinit const auto Logger = TabletServerLogger;

////////////////////////////////////////////////////////////////////////////////

class TTabletService
    : public ITabletService
    , public TMasterAutomatonPart
{
public:
    explicit TTabletService(TBootstrap* bootstrap)
        : TMasterAutomatonPart(bootstrap, EAutomatonThreadQueue::TabletManager)
    {
        YT_ASSERT_INVOKER_THREAD_AFFINITY(Bootstrap_->GetHydraFacade()->GetAutomatonInvoker(EAutomatonThreadQueue::Default), AutomatonThread);
    }

    void Initialize() override
    {
        const auto& transactionManager = Bootstrap_->GetTransactionManager();
        transactionManager->RegisterTransactionActionHandlers<TReqMount>({
            .Prepare = BIND_NO_PROPAGATE(&TTabletService::HydraPrepareMount, Unretained(this)),
            .Commit = BIND_NO_PROPAGATE(&TTabletService::HydraCommitMount, Unretained(this)),
            .Abort = BIND_NO_PROPAGATE(&TTabletService::HydraAbortMount, Unretained(this)),
        });

        transactionManager->RegisterTransactionActionHandlers<TReqUnmount>({
            .Prepare = BIND_NO_PROPAGATE(&TTabletService::HydraPrepareUnmount, Unretained(this)),
            .Commit = BIND_NO_PROPAGATE(&TTabletService::HydraCommitUnmount, Unretained(this)),
            .Abort = BIND_NO_PROPAGATE(&TTabletService::HydraAbortUnmount, Unretained(this)),
        });

        transactionManager->RegisterTransactionActionHandlers<TReqFreeze>({
            .Prepare = BIND_NO_PROPAGATE(&TTabletService::HydraPrepareFreeze, Unretained(this)),
            .Commit = BIND_NO_PROPAGATE(&TTabletService::HydraCommitFreeze, Unretained(this)),
            .Abort = BIND_NO_PROPAGATE(&TTabletService::HydraAbortFreeze, Unretained(this)),
        });

        transactionManager->RegisterTransactionActionHandlers<TReqUnfreeze>({
            .Prepare = BIND_NO_PROPAGATE(&TTabletService::HydraPrepareUnfreeze, Unretained(this)),
            .Commit = BIND_NO_PROPAGATE(&TTabletService::HydraCommitUnfreeze, Unretained(this)),
            .Abort = BIND_NO_PROPAGATE(&TTabletService::HydraAbortUnfreeze, Unretained(this)),
        });

        transactionManager->RegisterTransactionActionHandlers<TReqRemount>({
            .Prepare = BIND_NO_PROPAGATE(&TTabletService::HydraPrepareRemount, Unretained(this)),
            .Commit = BIND_NO_PROPAGATE(&TTabletService::HydraCommitRemount, Unretained(this)),
            .Abort = BIND_NO_PROPAGATE(&TTabletService::HydraAbortRemount, Unretained(this)),
        });

        transactionManager->RegisterTransactionActionHandlers<TReqReshard>({
            .Prepare = BIND_NO_PROPAGATE(&TTabletService::HydraPrepareReshard, Unretained(this)),
            .Commit = BIND_NO_PROPAGATE(&TTabletService::HydraCommitReshard, Unretained(this)),
            .Abort = BIND_NO_PROPAGATE(&TTabletService::HydraAbortReshard, Unretained(this)),
        });

        transactionManager->RegisterTransactionActionHandlers<TReqTwoPhaseAlter>({
            .Prepare = BIND_NO_PROPAGATE(&TTabletService::HydraPrepareAlter, Unretained(this)),
            .Commit = BIND_NO_PROPAGATE(&TTabletService::HydraCommitAlter, Unretained(this)),
            .Abort = BIND_NO_PROPAGATE(&TTabletService::HydraAbortAlter, Unretained(this)),
        });
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

    static TTableNode* AsTableSafe(TCypressNode* node)
    {
        if (!node) {
            return nullptr;
        }
        if (!IsTableType(node->GetType())) {
            THROW_ERROR_EXCEPTION("%v is not a table", node->GetId());
        }
        return node->As<TTableNode>();
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

        YT_TLOG_DEBUG("Preparing table mount")
            .With("TableId", tableId)
            .With("TransactionId", transaction->GetId())
            .With("AuthenticationIdentity", NRpc::GetCurrentAuthenticationIdentity())
            .With("FirstTabletIndex", firstTabletIndex)
            .With("LastTabletIndex", lastTabletIndex)
            .With("CellId", hintCellId)
            .With("TargetCellIds", targetCellIds)
            .With("Freeze", freeze)
            .With("MountTimestamp", mountTimestamp);

        ValidateNoParentTransaction(transaction);

        const auto& cypressManager = Bootstrap_->GetCypressManager();
        auto* table = AsTabletOwnerSafe(cypressManager->GetNodeOrThrow(TVersionedNodeId(tableId)));

        table->ValidateNoCurrentMountTransaction(Format("Cannot mount %v", table->GetLowercaseObjectName()));

        if (table->IsNative()) {
            auto currentPath = cypressManager->GetNodePath(table, nullptr);
            if (path != currentPath) {
                THROW_ERROR_EXCEPTION("%v path mismatch", table->GetCapitalizedObjectName())
                    .With("requested_path", path)
                    .With("resolved_path", currentPath);
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

        YT_TLOG_DEBUG("Committing table mount")
            .With("TableId", tableId)
            .With("TransactionId", transaction->GetId())
            .With("AuthenticationIdentity", NRpc::GetCurrentAuthenticationIdentity())
            .With("FirstTabletIndex", firstTabletIndex)
            .With("LastTabletIndex", lastTabletIndex)
            .With("CellId", hintCellId)
            .With("TargetCellIds", targetCellIds)
            .With("Freeze", freeze)
            .With("MountTimestamp", mountTimestamp);

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

        YT_TLOG_DEBUG("Aborting table mount")
            .With("TableId", tableId)
            .With("TransactionId", transaction->GetId())
            .With("AuthenticationIdentity", NRpc::GetCurrentAuthenticationIdentity())
            .With("FirstTabletIndex", firstTabletIndex)
            .With("LastTabletIndex", lastTabletIndex)
            .With("CellId", hintCellId)
            .With("TargetCellIds", targetCellIds)
            .With("Freeze", freeze)
            .With("MountTimestamp", mountTimestamp);

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

        YT_TLOG_DEBUG("Preparing table unmount")
            .With("TableId", tableId)
            .With("TransactionId", transaction->GetId())
            .With("AuthenticationIdentity", NRpc::GetCurrentAuthenticationIdentity())
            .With("Force", force)
            .With("FirstTabletIndex", firstTabletIndex)
            .With("LastTabletIndex", lastTabletIndex);

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

        YT_TLOG_DEBUG("Committing table unmount")
            .With("TableId", tableId)
            .With("TransactionId", transaction->GetId())
            .With("AuthenticationIdentity", NRpc::GetCurrentAuthenticationIdentity())
            .With("Force", force)
            .With("FirstTabletIndex", firstTabletIndex)
            .With("LastTabletIndex", lastTabletIndex);

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

        YT_TLOG_DEBUG("Aborting table unmount")
            .With("TableId", tableId)
            .With("TransactionId", transaction->GetId())
            .With("AuthenticationIdentity", NRpc::GetCurrentAuthenticationIdentity())
            .With("Force", force)
            .With("FirstTabletIndex", firstTabletIndex)
            .With("LastTabletIndex", lastTabletIndex);

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

        YT_TLOG_DEBUG("Preparing table freeze")
            .With("TableId", tableId)
            .With("TransactionId", transaction->GetId())
            .With("AuthenticationIdentity", NRpc::GetCurrentAuthenticationIdentity())
            .With("FirstTabletIndex", firstTabletIndex)
            .With("LastTabletIndex", lastTabletIndex);

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

        YT_TLOG_DEBUG("Committing table freeze")
            .With("TableId", tableId)
            .With("TransactionId", transaction->GetId())
            .With("AuthenticationIdentity", NRpc::GetCurrentAuthenticationIdentity())
            .With("FirstTabletIndex", firstTabletIndex)
            .With("LastTabletIndex", lastTabletIndex);

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

        YT_TLOG_DEBUG("Aborting table freeze")
            .With("TableId", tableId)
            .With("TransactionId", transaction->GetId())
            .With("AuthenticationIdentity", NRpc::GetCurrentAuthenticationIdentity())
            .With("FirstTabletIndex", firstTabletIndex)
            .With("LastTabletIndex", lastTabletIndex);

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

        YT_TLOG_DEBUG("Preparing table unfreeze")
            .With("TableId", tableId)
            .With("TransactionId", transaction->GetId())
            .With("AuthenticationIdentity", NRpc::GetCurrentAuthenticationIdentity())
            .With("FirstTabletIndex", firstTabletIndex)
            .With("LastTabletIndex", lastTabletIndex);

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

        YT_TLOG_DEBUG("Committing table unfreeze")
            .With("TableId", tableId)
            .With("TransactionId", transaction->GetId())
            .With("AuthenticationIdentity", NRpc::GetCurrentAuthenticationIdentity())
            .With("FirstTabletIndex", firstTabletIndex)
            .With("LastTabletIndex", lastTabletIndex);

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

        YT_TLOG_DEBUG("Aborting table unfreeze")
            .With("TableId", tableId)
            .With("TransactionId", transaction->GetId())
            .With("AuthenticationIdentity", NRpc::GetCurrentAuthenticationIdentity())
            .With("FirstTabletIndex", firstTabletIndex)
            .With("LastTabletIndex", lastTabletIndex);

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

        YT_TLOG_DEBUG("Preparing table remount")
            .With("TableId", tableId)
            .With("TransactionId", transaction->GetId())
            .With("AuthenticationIdentity", NRpc::GetCurrentAuthenticationIdentity())
            .With("FirstTabletIndex", firstTabletIndex)
            .With("LastTabletIndex", lastTabletIndex);

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

        YT_TLOG_DEBUG("Committing table remount")
            .With("TableId", tableId)
            .With("TransactionId", transaction->GetId())
            .With("AuthenticationIdentity", NRpc::GetCurrentAuthenticationIdentity())
            .With("FirstTabletIndex", firstTabletIndex)
            .With("LastTabletIndex", lastTabletIndex);

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

        YT_TLOG_DEBUG("Aborting table remount")
            .With("TableId", tableId)
            .With("TransactionId", transaction->GetId())
            .With("AuthenticationIdentity", NRpc::GetCurrentAuthenticationIdentity())
            .With("FirstTabletIndex", firstTabletIndex)
            .With("LastTabletIndex", lastTabletIndex);

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
        auto cumulativeDataWeights = FromProto<std::vector<i64>>(request->cumulative_data_weights());

        const auto& securityManager = Bootstrap_->GetSecurityManager();
        TAuthenticatedUserGuard userGuard(securityManager);

        YT_TLOG_DEBUG("Preparing table reshard")
            .With("TableId", tableId)
            .With("TransactionId", transaction->GetId())
            .With("AuthenticationIdentity", NRpc::GetCurrentAuthenticationIdentity())
            .With("TabletCount", tabletCount)
            .With("PivotKeysSize", pivotKeys.size())
            .With("TrimmedRowCountsSize", trimmedRowCounts.size())
            .With("CumulativeDataWeightsSize", cumulativeDataWeights.size())
            .With("FirstTabletIndex", firstTabletIndex)
            .With("LastTabletIndex", lastTabletIndex);

        ValidateNoParentTransaction(transaction);

        const auto& cypressManager = Bootstrap_->GetCypressManager();
        auto* table = AsTabletOwnerSafe(cypressManager->GetNodeOrThrow(TVersionedNodeId(tableId)));

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
            trimmedRowCounts,
            cumulativeDataWeights);

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
        auto cumulativeDataWeights = FromProto<std::vector<i64>>(request->cumulative_data_weights());

        YT_TLOG_DEBUG("Committing table reshard")
            .With("TableId", tableId)
            .With("TransactionId", transaction->GetId())
            .With("AuthenticationIdentity", NRpc::GetCurrentAuthenticationIdentity())
            .With("TabletCount", tabletCount)
            .With("PivotKeysSize", pivotKeys.size())
            .With("TrimmedRowCountsSize", trimmedRowCounts.size())
            .With("CumulativeDataWeightsSize", cumulativeDataWeights.size())
            .With("FirstTabletIndex", firstTabletIndex)
            .With("LastTabletIndex", lastTabletIndex);

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
            trimmedRowCounts,
            cumulativeDataWeights);

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

        YT_TLOG_DEBUG("Aborting table reshard")
            .With("TableId", tableId)
            .With("TransactionId", transaction->GetId())
            .With("AuthenticationIdentity", NRpc::GetCurrentAuthenticationIdentity())
            .With("TabletCount", tabletCount)
            .With("PivotKeysSize", pivotKeys.size())
            .With("FirstTabletIndex", firstTabletIndex)
            .With("LastTabletIndex", lastTabletIndex);

        const auto& cypressManager = Bootstrap_->GetCypressManager();
        auto* table = AsTabletOwnerSafe(cypressManager->FindNode(TVersionedNodeId(tableId)));

        if (!IsObjectAlive(table)) {
            return;
        }

        table->UnlockCurrentMountTransaction(transaction->GetId());

        YT_LOG_ACCESS(tableId, cypressManager->GetNodePath(table, nullptr), transaction, "AbortReshard");
    }

    void HydraPrepareAlter(
        TTransaction* transaction,
        NTabletClient::NProto::TReqTwoPhaseAlter* request,
        const TTransactionPrepareOptions& /*options*/)
    {
        auto tableId = FromProto<TTableId>(request->table_id());
        const auto& path = request->path();
        bool dynamic = request->dynamic();

        const auto& securityManager = Bootstrap_->GetSecurityManager();
        TAuthenticatedUserGuard userGuard(securityManager);

        YT_TLOG_DEBUG("Preparing table alter")
            .With("TableId", tableId)
            .With("TransactionId", transaction->GetId())
            .With("Dynamic", dynamic)
            .With("AuthenticationIdentity", NRpc::GetCurrentAuthenticationIdentity());

        ValidateNoParentTransaction(transaction);

        const auto& cypressManager = Bootstrap_->GetCypressManager();
        auto* table = AsTableSafe(cypressManager->GetNodeOrThrow(TVersionedNodeId(tableId)));

        if (table->IsSequoia()) {
            THROW_ERROR_EXCEPTION("Two-phase table alter is not supported for Sequoia tables");
        }

        table->ValidateNoCurrentMountTransaction(
            Format("Cannot alter dynamicity of %v", table->GetLowercaseObjectName()));

        const auto& tabletManager = Bootstrap_->GetTabletManager();
        if (table->IsNative()) {
            auto currentPath = cypressManager->GetNodePath(table, nullptr);
            if (path != currentPath) {
                THROW_ERROR_EXCEPTION("%v path mismatch", table->GetCapitalizedObjectName())
                    .With("requested_path", path)
                    .With("resolved_path", currentPath);
            }

            securityManager->ValidatePermission(table, EPermission::Write);

            const auto& config = Bootstrap_->GetConfigManager()->GetConfig();
            switch (config->SecurityManager->AllowAlterWithoutFullRead) {
                case EAllowAlterWithoutFullRead::Allow: {
                    break;
                }
                case EAllowAlterWithoutFullRead::Deny: {
                    securityManager->ValidatePermission(table, EPermission::FullRead);
                    break;
                }
                case EAllowAlterWithoutFullRead::AllowAndAlert: {
                    try {
                        securityManager->ValidatePermission(table, EPermission::FullRead);
                    } catch (const std::exception& ex) {
                        YT_TLOG_ALERT("User requested to alter table but lacks \"full_read\" permission")
                            .With("TableId", tableId)
                            .With("User", securityManager->GetAuthenticatedUser())
                            .With(ex);
                    }
                    break;
                }
            }

            cypressManager->LockNode(table, transaction, ELockMode::Exclusive, false, true);
        }

        tabletManager->PrepareAlter(table, dynamic);

        table->LockCurrentMountTransaction(transaction->GetId());

        YT_LOG_ACCESS(tableId, path, transaction, "PrepareAlter");
    }

    void HydraCommitAlter(
        TTransaction* transaction,
        NTabletClient::NProto::TReqTwoPhaseAlter* request,
        const TTransactionCommitOptions& /*options*/)
    {
        auto tableId = FromProto<TTableId>(request->table_id());
        bool dynamic = request->dynamic();

        YT_TLOG_DEBUG("Committing table alter")
            .With("TableId", tableId)
            .With("TransactionId", transaction->GetId())
            .With("Dynamic", dynamic)
            .With("AuthenticationIdentity", NRpc::GetCurrentAuthenticationIdentity());

        const auto& cypressManager = Bootstrap_->GetCypressManager();
        auto* table = AsTableSafe(cypressManager->FindNode(TVersionedNodeId(tableId)));

        if (!IsObjectAlive(table)) {
            return;
        }

        table->UnlockCurrentMountTransaction(transaction->GetId());

        const auto& tabletManager = Bootstrap_->GetTabletManager();
        tabletManager->Alter(table, dynamic);

        YT_LOG_ACCESS(tableId, request->path(), transaction, "CommitAlter");
    }

    void HydraAbortAlter(
        TTransaction* transaction,
        NTabletClient::NProto::TReqTwoPhaseAlter* request,
        const TTransactionAbortOptions& /*options*/)
    {
        auto tableId = FromProto<TTableId>(request->table_id());
        bool dynamic = request->dynamic();

        YT_TLOG_DEBUG("Aborting table alter")
            .With("TableId", tableId)
            .With("TransactionId", transaction->GetId())
            .With("Dynamic", dynamic)
            .With("AuthenticationIdentity", NRpc::GetCurrentAuthenticationIdentity());

        const auto& cypressManager = Bootstrap_->GetCypressManager();
        auto* table = AsTableSafe(cypressManager->FindNode(TVersionedNodeId(tableId)));

        if (!IsObjectAlive(table)) {
            return;
        }

        table->UnlockCurrentMountTransaction(transaction->GetId());

        YT_LOG_ACCESS(tableId, request->path(), transaction, "AbortAlter");
    }
};

////////////////////////////////////////////////////////////////////////////////

ITabletServicePtr CreateTabletService(TBootstrap* bootstrap)
{
    return New<TTabletService>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletServer
