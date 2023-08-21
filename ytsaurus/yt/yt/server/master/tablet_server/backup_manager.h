#pragma once

#include "public.h"

#include <yt/yt/server/master/cell_master/public.h>
#include <yt/yt/server/master/table_server/public.h>
#include <yt/yt/server/master/transaction_server/public.h>

#include <yt/yt/ytlib/cypress_client/public.h>

#include <yt/yt/ytlib/tablet_client/backup.h>

#include <yt/yt/client/api/public.h>

namespace NYT::NTabletServer {

////////////////////////////////////////////////////////////////////////////////

struct IBackupManager
    : public virtual TRefCounted
{
    virtual void Initialize() = 0;

    virtual void StartBackup(
        NTableServer::TTableNode* table,
        NTransactionClient::TTimestamp timestamp,
        NTransactionServer::TTransaction* transaction,
        NTabletClient::EBackupMode backupMode,
        TTableReplicaId upstreamReplicaId,
        std::optional<NApi::TClusterTag> clockClusterTag,
        std::vector<NTabletClient::TTableReplicaBackupDescriptor>
            replicaBackupDescriptors) = 0;

    virtual void StartRestore(
        NTableServer::TTableNode* table,
        NTransactionServer::TTransaction* transaction,
        std::vector<NTabletClient::TTableReplicaBackupDescriptor>
            replicaBackupDescriptors) = 0;

    virtual void ReleaseBackupCheckpoint(
        NTableServer::TTableNode* table,
        NTransactionServer::TTransaction* transaction) = 0;

    virtual void CheckBackup(
        NTableServer::TTableNode* table,
        NTableClient::NProto::TRspCheckBackup* response) = 0;

    virtual TFuture<void> FinishBackup(NTableServer::TTableNode* table) = 0;

    virtual TFuture<void> FinishRestore(NTableServer::TTableNode* table) = 0;

    virtual void SetClonedTabletBackupState(
        TTablet* clonedTablet,
        const TTablet* sourceTablet,
        NCypressClient::ENodeCloneMode mode) = 0;

    virtual void UpdateAggregatedBackupState(NTableServer::TTableNode* table) = 0;

    virtual void OnBackupInterruptedByUnmount(TTablet* tablet) = 0;
};

DEFINE_REFCOUNTED_TYPE(IBackupManager)

////////////////////////////////////////////////////////////////////////////////

IBackupManagerPtr CreateBackupManager(NCellMaster::TBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletServer
