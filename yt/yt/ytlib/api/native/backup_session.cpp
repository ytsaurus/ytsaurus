#include "backup_session.h"

#include "client.h"
#include "transaction.h"

#include <yt/yt/ytlib/cypress_client/rpc_helpers.h>

#include <yt/yt/ytlib/table_client/helpers.h>

#include <yt/yt/ytlib/transaction_client/helpers.h>

#include <yt/yt/client/transaction_client/helpers.h>

namespace NYT::NApi::NNative {

using namespace NConcurrency;
using namespace NCypressClient;
using namespace NObjectClient;
using namespace NTableClient;
using namespace NTabletClient;
using namespace NTransactionClient;
using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

constexpr static const char* OriginalTabletStateAttributeName = "original_tablet_state";
constexpr static const char* OriginalEnabledReplicaIdsAttributeName  = "original_enabled_replica_ids";

////////////////////////////////////////////////////////////////////////////////

TClusterBackupSession::TClusterBackupSession(
    TString clusterName,
    TClientPtr client,
    TCreateOrRestoreTableBackupOptions options,
    TTimestamp timestamp,
    EBackupDirection direction,
    NLogging::TLogger logger)
    : ClusterName_(std::move(clusterName))
    , Client_(std::move(client))
    , Options_(options)
    , Timestamp_(timestamp)
    , Direction_(direction)
    , Logger(logger
        .WithTag("SessionId: %v", TGuid::Create())
        .WithTag("Cluster: %v", ClusterName_))
{ }

TClusterBackupSession::~TClusterBackupSession()
{
    if (Transaction_) {
        YT_LOG_DEBUG("Aborting backup transaction due to session failure");
        YT_UNUSED_FUTURE(Transaction_->Abort());
    }
}

void TClusterBackupSession::ValidateBackupsEnabled()
{
    auto rspOrError = WaitFor(Client_->GetNode("//sys/@config/tablet_manager/enable_backups", {}));

    if (rspOrError.IsOK()) {
        auto rsp = ConvertTo<bool>(rspOrError.Value());
        if (rsp) {
            return;
        }
    } else if (!rspOrError.FindMatching(NYTree::EErrorCode::ResolveError)) {
        THROW_ERROR_EXCEPTION("Failed to check if backups are enabled at cluster %Qv",
            ClusterName_)
            << rspOrError;
    }

    THROW_ERROR_EXCEPTION("Backups are disabled at cluster %Qv",
        ClusterName_);
}

void TClusterBackupSession::RegisterTable(const TTableBackupManifestPtr& manifest)
{
    TTableInfo tableInfo;
    tableInfo.SourcePath = manifest->SourcePath;
    tableInfo.DestinationPath = manifest->DestinationPath;

    tableInfo.Attributes = ResolveExternalTable(
        Client_,
        tableInfo.SourcePath,
        &tableInfo.SourceTableId,
        &tableInfo.ExternalCellTag,
        {
            "sorted",
            "upstream_replica_id",
            "replicas",
            "dynamic",
            "commit_ordering",
            "tablet_state",
            OriginalTabletStateAttributeName,
            OriginalEnabledReplicaIdsAttributeName,
        });

    auto dynamic = tableInfo.Attributes->Get<bool>("dynamic");

    if (!dynamic) {
        THROW_ERROR_EXCEPTION("Cannot backup static table %v", tableInfo.SourcePath)
            << TErrorAttribute("cluster_name", ClusterName_);
    }

    auto sorted = tableInfo.Attributes->Get<bool>("sorted");
    auto commitOrdering = tableInfo.Attributes->Get<ECommitOrdering>("commit_ordering");
    auto type = TypeFromId(tableInfo.SourceTableId);
    bool replicated = type == EObjectType::ReplicatedTable;
    auto upstreamReplicaId = tableInfo.Attributes->Get<TTableReplicaId>("upstream_replica_id");

    tableInfo.UpstreamReplicaId = upstreamReplicaId;
    tableInfo.Sorted = sorted;
    tableInfo.Replicated = replicated;
    tableInfo.CommitOrdering = commitOrdering;
    tableInfo.OrderedTableBackupMode = manifest->OrderedMode;

    if (Direction_ == EBackupDirection::Backup) {
        tableInfo.TabletState = tableInfo.Attributes->Get<ETabletState>("tablet_state");
    } else {
        tableInfo.TabletState = tableInfo.Attributes->Get<ETabletState>(
            OriginalTabletStateAttributeName,
            ETabletState::Unmounted);
    }

    try {
        if (type == EObjectType::ReplicatedTable) {
            auto replicas = tableInfo.Attributes->Get<
                THashMap<TTableReplicaId, INodePtr>>("replicas");
            auto enabledReplicaIds = tableInfo.Attributes->Get<THashSet<TTableReplicaId>>(
                OriginalEnabledReplicaIdsAttributeName,
                {});
            for (const auto& [replicaId, attributesString] : replicas) {
                auto attributes = ConvertToAttributes(attributesString);
                TTableReplicaInfo replicaInfo{
                    .Id = replicaId,
                    .ClusterName = attributes->Get<TString>("cluster_name"),
                    .Mode = attributes->Get<ETableReplicaMode>("mode"),
                    .ReplicaPath = attributes->Get<TString>("replica_path"),
                };

                if (Direction_ == EBackupDirection::Backup) {
                    replicaInfo.State = attributes->Get<ETableReplicaState>("state");
                } else {
                    replicaInfo.State = enabledReplicaIds.contains(replicaId)
                        ? ETableReplicaState::Enabled
                        : ETableReplicaState::Disabled;
                }

                tableInfo.Replicas[replicaId] = replicaInfo;

                if (replicaInfo.Mode != ETableReplicaMode::Sync &&
                    replicaInfo.Mode != ETableReplicaMode::Async)
                {
                    THROW_ERROR_EXCEPTION("Replica %v of table %v has unsupported mode %Qlv",
                        replicaId,
                        tableInfo.SourcePath,
                        replicaInfo.Mode);
                }
            }
        }

        if (!SourceTableIds_.insert(tableInfo.SourceTableId).second) {
            THROW_ERROR_EXCEPTION("Duplicate table %Qv in backup manifest",
                tableInfo.SourcePath);
        }

        if (type == EObjectType::ReplicationLogTable) {
            THROW_ERROR_EXCEPTION("Table %Qv is a replication log",
                tableInfo.SourcePath);
        }

        if (type != EObjectType::Table && type != EObjectType::ReplicatedTable) {
            THROW_ERROR_EXCEPTION("Cannot backup table %Qv of type %Qlv",
                tableInfo.SourcePath,
                type);
        }

        if (!replicated) {
            if (sorted) {
                if (commitOrdering != ECommitOrdering::Weak) {
                    THROW_ERROR_EXCEPTION("Sorted table %Qv has unsupported commit ordering %Qlv",
                        tableInfo.SourcePath,
                        commitOrdering);
                }
            }
        }

        if (tableInfo.UpstreamReplicaId && !sorted) {
            THROW_ERROR_EXCEPTION("Cannot backup ordered replica table %v",
                tableInfo.SourcePath);
        }

        if (replicated && !sorted) {
            THROW_ERROR_EXCEPTION("Cannot backup ordered replicated table %v",
                tableInfo.SourcePath);
        }

        if (CellTagFromId(tableInfo.SourceTableId) !=
            Client_->GetNativeConnection()->GetPrimaryMasterCellTag())
        {
            THROW_ERROR_EXCEPTION("Table %Qv is beyond the portal",
                tableInfo.SourcePath);
        }

        if (Direction_ == EBackupDirection::Restore && GetRestoreOptions().Mount) {
            switch (tableInfo.TabletState) {
                case ETabletState::Unmounted:
                case ETabletState::Frozen:
                case ETabletState::Mounted:
                    break;

                case ETabletState::Mixed:
                    THROW_ERROR_EXCEPTION("Cannot mount restored table %Qv since "
                        "original table tablets were in different states",
                        tableInfo.SourcePath);

                default:
                    THROW_ERROR_EXCEPTION("Cannot mount restored table %Qv since "
                        "original table had unsupported tablet state %Qlv",
                        tableInfo.SourcePath,
                        tableInfo.TabletState);
            }
        }
    } catch (const TErrorException& e) {
        ThrowWithClusterNameIfFailed(e);
    }

    CellTags_.insert(CellTagFromId(tableInfo.SourceTableId));
    CellTags_.insert(tableInfo.ExternalCellTag);

    TableIndexesByCellTag_[tableInfo.ExternalCellTag].push_back(ssize(Tables_));
    Tables_.push_back(std::move(tableInfo));
}

void TClusterBackupSession::StartTransaction()
{
    auto title = Direction_== EBackupDirection::Backup
        ? "Create backup"
        : "Restore backup";

    auto transactionAttributes = CreateEphemeralAttributes();
    transactionAttributes->Set("title", title);
    TTransactionStartOptions options;
    options.Attributes = std::move(transactionAttributes);
    options.ReplicateToMasterCellTags = TCellTagList(CellTags_.begin(), CellTags_.end());

    auto asyncTransaction = Client_->StartNativeTransaction(
        NTransactionClient::ETransactionType::Master,
        options);
    Transaction_ = WaitFor(asyncTransaction)
        .ValueOrThrow();

    auto primaryMasterCellTag = Client_->GetNativeConnection()->GetPrimaryMasterCellTag();
    if (CellTagFromId(Transaction_->GetId()) != primaryMasterCellTag) {
        ExternalizedViaPrimaryCellTransactionId_ = MakeExternalizedTransactionId(
            Transaction_->GetId(),
            primaryMasterCellTag);
    } else {
        ExternalizedViaPrimaryCellTransactionId_ = Transaction_->GetId();
    }
}

void TClusterBackupSession::LockInputTables()
{
    TLockNodeOptions options;
    options.TransactionId = Transaction_->GetId();

    std::vector<TFuture<TLockNodeResult>> asyncRsps;
    for (const auto& table : Tables_) {
        asyncRsps.push_back(Client_->LockNode(table.SourcePath, ELockMode::Exclusive, options));
    }

    auto rspsOrErrors = WaitFor(AllSucceeded(asyncRsps));
    ThrowWithClusterNameIfFailed(rspsOrErrors);

    for (int tableIndex = 0; tableIndex < ssize(Tables_); ++tableIndex) {
        const auto& rsp = rspsOrErrors.Value()[tableIndex];
        if (rsp.NodeId != Tables_[tableIndex].SourceTableId) {
            THROW_ERROR_EXCEPTION("Table id changed during locking")
                << TErrorAttribute("table_path", Tables_[tableIndex].SourcePath)
                << TErrorAttribute("cluster_name", ClusterName_);
        }
    }
}

void TClusterBackupSession::StartBackup()
{
    auto buildRequest = [&] (const auto& batchReq, const TTableInfo& table) {
        auto req = TTableYPathProxy::StartBackup(FromObjectId(table.SourceTableId));
        req->set_timestamp(Timestamp_);
        SetTransactionId(req, GetExternalizedTransactionId(table));

        EBackupMode mode;

        if (table.Replicated) {
            YT_VERIFY(table.Sorted);
            mode = EBackupMode::ReplicatedSorted;
        } else if (table.Sorted) {
            if (table.UpstreamReplica) {
                switch (table.ReplicaMode) {
                    case ETableReplicaMode::Sync:
                        mode = EBackupMode::SortedSyncReplica;
                        break;

                    case ETableReplicaMode::Async:
                        mode = EBackupMode::SortedAsyncReplica;
                        break;

                    default:
                        YT_ABORT();
                }
            } else {
                mode = EBackupMode::Sorted;
            }
        } else {
            if (table.CommitOrdering == ECommitOrdering::Strong) {
                mode = EBackupMode::OrderedStrongCommitOrdering;
            } else {
                switch (table.OrderedTableBackupMode) {
                    case EOrderedTableBackupMode::Exact:
                        mode = EBackupMode::OrderedExact;
                        break;

                    case EOrderedTableBackupMode::AtLeast:
                        mode = EBackupMode::OrderedAtLeast;
                        break;

                    case EOrderedTableBackupMode::AtMost:
                        mode = EBackupMode::OrderedAtMost;
                        break;

                    default:
                        YT_ABORT();
                }
            }
        }

        req->set_backup_mode(static_cast<int>(mode));

        if (table.UpstreamReplicaId) {
            ToProto(req->mutable_upstream_replica_id(), table.UpstreamReplicaId);
            req->set_clock_cluster_tag(ToProto<int>(table.ClockClusterTag));
        }

        ToProto(req->mutable_replicas(), table.BackupableReplicas);

        batchReq->AddRequest(req, ToString(table.SourceTableId));
    };

    auto onResponse = [&] (const auto& batchRsp, TTableInfo* tableInfo) {
        const auto& rsp = batchRsp->template GetResponse<TTableYPathProxy::TRspStartBackup>(
            ToString(tableInfo->SourceTableId));
        ThrowWithClusterNameIfFailed(rsp);
    };

    ExecuteForAllTables(buildRequest, onResponse, /*write*/ true);
}

void TClusterBackupSession::StartRestore()
{
    auto buildRequest = [&] (const auto& batchReq, const TTableInfo& table) {
        auto req = TTableYPathProxy::StartRestore(FromObjectId(table.SourceTableId));
        SetTransactionId(req, GetExternalizedTransactionId(table));
        ToProto(req->mutable_replicas(), table.BackupableReplicas);
        batchReq->AddRequest(req, ToString(table.SourceTableId));
    };

    auto onResponse = [&] (const auto& batchRsp, TTableInfo* tableInfo) {
        const auto& rsp = batchRsp->template GetResponse<TTableYPathProxy::TRspStartRestore>(
            ToString(tableInfo->SourceTableId));
        ThrowWithClusterNameIfFailed(rsp);
    };

    ExecuteForAllTables(buildRequest, onResponse, /*write*/ true);
}

void TClusterBackupSession::WaitForCheckpoint()
{
    THashSet<const TTableInfo*> unconfirmedTables;
    for (auto& table : Tables_) {
        unconfirmedTables.insert(&table);
    }

    auto buildRequest = [&] (const auto& batchReq, const TTableInfo& table) {
        if (!unconfirmedTables.contains(&table)) {
            return;
        }

        auto req = TTableYPathProxy::CheckBackup(FromObjectId(table.SourceTableId));
        SetTransactionId(req, GetExternalizedTransactionId(table));
        batchReq->AddRequest(req, ToString(table.SourceTableId));
    };

    auto onResponse = [&] (const auto& batchRsp, TTableInfo* table) {
        if (!unconfirmedTables.contains(table)) {
            return;
        }

        const auto& rspOrError = batchRsp->template GetResponse<TTableYPathProxy::TRspCheckBackup>(
            ToString(table->SourceTableId));
        const auto& rsp = rspOrError.ValueOrThrow();
        auto confirmedTabletCount = rsp->confirmed_tablet_count();
        auto pendingTabletCount = rsp->pending_tablet_count();

        YT_LOG_DEBUG("Backup checkpoint checked (TablePath: %v, ConfirmedTabletCount: %v, "
            "PendingTabletCount: %v)",
            table->SourcePath,
            confirmedTabletCount,
            pendingTabletCount);

        if (pendingTabletCount == 0) {
            unconfirmedTables.erase(table);
        }
    };

    const auto& options = GetCreateOptions();
    auto deadline = TInstant::Now() + options.CheckpointCheckTimeout;

    while (TInstant::Now() < deadline) {
        YT_LOG_DEBUG("Waiting for backup checkpoint (RemainingTableCount: %v)",
            ssize(unconfirmedTables));
        ExecuteForAllTables(buildRequest, onResponse, /*write*/ false);
        if (unconfirmedTables.empty()) {
            break;
        }
        TDelayedExecutor::WaitForDuration(options.CheckpointCheckPeriod);
    }

    if (!unconfirmedTables.empty()) {
        THROW_ERROR_EXCEPTION("Some tables did not confirm backup checkpoint passing within timeout")
            << TErrorAttribute("remaining_table_count", ssize(unconfirmedTables))
            << TErrorAttribute("sample_table_path", (*unconfirmedTables.begin())->SourcePath)
            << TErrorAttribute("cluster_name", ClusterName_);
    }
}

void TClusterBackupSession::CloneTables(ENodeCloneMode nodeCloneMode)
{
    TCopyNodeOptions options;
    options.TransactionId = Transaction_->GetId();

    bool force;
    bool preserveAccount;
    if (nodeCloneMode == ENodeCloneMode::Backup) {
        force = GetCreateOptions().Force;
        preserveAccount = GetCreateOptions().PreserveAccount;
    } else {
        force = GetRestoreOptions().Force;
        preserveAccount = GetRestoreOptions().PreserveAccount;
    }

    // TODO(ifsmirnov): this doesn't work for tables beyond the portals.
    auto proxy = Client_->CreateObjectServiceWriteProxy();
    auto batchReq = proxy.ExecuteBatch();

    for (const auto& table : Tables_) {
        auto req = TCypressYPathProxy::Copy(table.DestinationPath);
        req->set_mode(static_cast<int>(nodeCloneMode));
        req->set_force(force);
        req->set_preserve_account(preserveAccount);
        Client_->SetTransactionId(req, options, /*allowNullTransaction*/ false);
        Client_->SetMutationId(req, options);
        auto* ypathExt = req->Header().MutableExtension(NYTree::NProto::TYPathHeaderExt::ypath_header_ext);
        ypathExt->add_additional_paths(table.SourcePath);
        batchReq->AddRequest(req);
    }

    auto batchRsp = WaitFor(batchReq->Invoke())
        .ValueOrThrow();

    auto rsps = batchRsp->GetResponses<TCypressYPathProxy::TRspCopy>();
    YT_VERIFY(ssize(rsps) == ssize(Tables_));

    for (int tableIndex = 0; tableIndex < ssize(Tables_); ++tableIndex) {
        const auto& rspOrError = rsps[tableIndex];
        auto& table = Tables_[tableIndex];

        if (rspOrError.GetCode() == NObjectClient::EErrorCode::CrossCellAdditionalPath) {
            THROW_ERROR_EXCEPTION("Cross-cell backups are not supported")
                << TErrorAttribute("source_path", table.SourcePath)
                << TErrorAttribute("destination_path", table.DestinationPath)
                << TErrorAttribute("cluster_name", ClusterName_);
        }

        const auto& rsp = rspOrError.ValueOrThrow();
        table.DestinationTableId = FromProto<TTableId>(rsp->node_id());
    }
}

void TClusterBackupSession::FinishBackups()
{
    auto buildRequest = [&] (const auto& batchReq, const TTableInfo& table) {
        auto req = TTableYPathProxy::FinishBackup(FromObjectId(table.DestinationTableId));
        SetTransactionId(req, GetExternalizedTransactionId(table));
        batchReq->AddRequest(req, ToString(table.DestinationTableId));
    };

    auto onResponse = [&] (const auto& batchRsp, TTableInfo* table) {
        const auto& rsp = batchRsp->template GetResponse<TTableYPathProxy::TRspFinishBackup>(
            ToString(table->DestinationTableId));
        ThrowWithClusterNameIfFailed(rsp);
    };

    ExecuteForAllTables(buildRequest, onResponse, /*write*/ true);
}

void TClusterBackupSession::FinishRestores()
{
    auto buildRequest = [&] (const auto& batchReq, const TTableInfo& table) {
        auto req = TTableYPathProxy::FinishRestore(FromObjectId(table.DestinationTableId));
        SetTransactionId(req, GetExternalizedTransactionId(table));
        batchReq->AddRequest(req, ToString(table.DestinationTableId));
    };

    auto onResponse = [&] (const auto& batchRsp, TTableInfo* table) {
        const auto& rsp = batchRsp->template GetResponse<TTableYPathProxy::TRspFinishRestore>(
            ToString(table->DestinationTableId));
        ThrowWithClusterNameIfFailed(rsp);
    };

    ExecuteForAllTables(buildRequest, onResponse, /*write*/ true);
}

void TClusterBackupSession::ValidateBackupStates(ETabletBackupState expectedState)
{
    auto buildRequest = [&] (const auto& batchReq, const TTableInfo& table) {
        auto req = TObjectYPathProxy::Get(FromObjectId(table.DestinationTableId) + "/@");
        const static std::vector<TString> ExtraAttributeKeys{"tablet_backup_state", "backup_error"};
        ToProto(req->mutable_attributes()->mutable_keys(), ExtraAttributeKeys);
        SetTransactionId(req, GetExternalizedTransactionId(table));
        batchReq->AddRequest(req, ToString(table.DestinationTableId));
    };

    auto onResponse = [&] (const auto& batchRsp, TTableInfo* table) {
        const auto& rspOrError = batchRsp->template GetResponse<TObjectYPathProxy::TRspGet>(
            ToString(table->DestinationTableId));
        const auto& rsp = rspOrError.ValueOrThrow();

        auto attributes = ConvertToAttributes(TYsonString(rsp->value()));

        auto actualState = attributes->Get<ETabletBackupState>("tablet_backup_state");
        auto optionalError = attributes->Find<TError>("backup_error");

        if (actualState != expectedState) {
            if (optionalError && !optionalError->IsOK()) {
                THROW_ERROR *optionalError
                    << TErrorAttribute("cluster_name", ClusterName_);

            }
            THROW_ERROR_EXCEPTION("Destination table %Qv has invalid backup state: expected %Qlv, got %Qlv",
                table->DestinationPath,
                expectedState,
                actualState)
                << TErrorAttribute("cluster_name", ClusterName_);
        }
    };

    TMasterReadOptions options;
    options.ReadFrom = EMasterChannelKind::Follower;
    ExecuteForAllTables(buildRequest, onResponse, /*write*/ false, options);
}

void TClusterBackupSession::FetchClonedReplicaIds()
{
    auto buildRequest = [&] (const auto& batchReq, const TTableInfo& table) {
        if (table.BackupableReplicas.empty()) {
            return;
        }

        auto req = TObjectYPathProxy::Get(FromObjectId(table.DestinationTableId) + "/@replicas");
        SetTransactionId(req, GetExternalizedTransactionId(table));
        batchReq->AddRequest(req, ToString(table.DestinationTableId));
    };

    auto onResponse = [&] (const auto& batchRsp, TTableInfo* table) {
        if (table->BackupableReplicas.empty()) {
            return;
        }

        const auto& rspOrError = batchRsp->template GetResponse<TObjectYPathProxy::TRspGet>(
            ToString(table->DestinationTableId));
        const auto& rsp = rspOrError.ValueOrThrow();

        auto clonedReplicas = ConvertTo<THashMap<TTableReplicaId, INodePtr>>(TYsonString(rsp->value()));
        for (const auto& [clonedReplicaId, attributesString] : clonedReplicas) {
            auto attributes = ConvertToAttributes(attributesString);
            auto replicaClusterName = attributes->template Get<TString>("cluster_name");
            auto replicaPath = attributes->template Get<TString>("replica_path");

            bool foundMatching = false;

            for (auto& [existingReplicaId, existingReplica] : table->Replicas) {
                if (replicaClusterName == existingReplica.ClusterName &&
                    replicaPath == existingReplica.ClonedReplicaPath)
                {
                    foundMatching = true;
                    existingReplica.ClonedReplicaId = clonedReplicaId;
                    break;
                }
            }

            if (!foundMatching) {
                THROW_ERROR_EXCEPTION("Cannot find matching source replica for a cloned replica %v "
                    "to table %v at cluster %Qv",
                    clonedReplicaId,
                    replicaPath,
                    replicaClusterName)
                    << TErrorAttribute("cluster_name", ClusterName_);
            }
        }
    };

    TMasterReadOptions options;
    options.ReadFrom = EMasterChannelKind::Follower;
    ExecuteForAllTables(buildRequest, onResponse, /*write*/ false, options);
}

void TClusterBackupSession::UpdateUpstreamReplicaIds()
{
    // Alter requests should go to native cell. Backups do not support portals yet,
    // so we use primary instead.
    auto proxy = Client_->CreateObjectServiceWriteProxy();
    auto batchReq = proxy.ExecuteBatch();

    for (const auto& table : Tables_) {
        if (!table.UpstreamReplica) {
            continue;
        }

        auto req = TTableYPathProxy::Alter(FromObjectId(table.DestinationTableId));
        SetTransactionId(req, Transaction_->GetId());
        ToProto(req->mutable_upstream_replica_id(), table.UpstreamReplica->ClonedReplicaId);
        batchReq->AddRequest(req);
    }

    auto rspOrError = WaitFor(batchReq->Invoke());
    ThrowWithClusterNameIfFailed(GetCumulativeError(rspOrError));
}

void TClusterBackupSession::RememberTabletStates()
{
    // Set-attribute requests should go to native cell. Backups do not support portals yet,
    // so we use primary instead.
    auto proxy = Client_->CreateObjectServiceWriteProxy();
    auto batchReq = proxy.ExecuteBatch();

    for (const auto& table : Tables_) {
        auto req = TObjectYPathProxy::Set(FromObjectId(
            table.DestinationTableId) + "/@" +
                OriginalTabletStateAttributeName);
        ToProto(
            req->mutable_value(),
            Format("%lv", table.TabletState));
        SetTransactionId(req, Transaction_->GetId());
        batchReq->AddRequest(req, ToString(table.DestinationTableId) + ":tablet_state");
    }

    for (const auto& table : Tables_) {
        std::vector<TTableReplicaId> enabledReplicaIds;
        for (const auto& [id, replicaInfo] : table.Replicas) {
            if (replicaInfo.State == ETableReplicaState::Enabled) {
                enabledReplicaIds.push_back(replicaInfo.ClonedReplicaId);
            }
        }

        if (!enabledReplicaIds.empty()) {
            auto req = TObjectYPathProxy::Set(FromObjectId(
                table.DestinationTableId) + "/@" +
                    OriginalEnabledReplicaIdsAttributeName);
            ToProto(
                req->mutable_value(),
                ConvertToYsonString(enabledReplicaIds).ToString());
            SetTransactionId(req, Transaction_->GetId());
            batchReq->AddRequest(req, ToString(table.DestinationTableId) + ":enabled_replica_ids");
        }
    }

    auto rspOrError = WaitFor(batchReq->Invoke());
    ThrowWithClusterNameIfFailed(GetCumulativeError(rspOrError));
}

void TClusterBackupSession::CommitTransaction()
{
    auto rsp = WaitFor(Transaction_->Commit());
    ThrowWithClusterNameIfFailed(rsp);
    Transaction_ = nullptr;
}

void TClusterBackupSession::MountRestoredTables()
{
    std::vector<TFuture<void>> asyncRsps;

    for (const auto& table : Tables_) {
        if (table.TabletState == ETabletState::Unmounted) {
            continue;
        }

        TMountTableOptions options;
        if (table.TabletState == ETabletState::Frozen) {
            options.Freeze = true;
        } else {
            YT_VERIFY(table.TabletState == ETabletState::Mounted);
        }

        YT_LOG_DEBUG("Mounting restored table (TableId: %v, TablePath: %v)",
            table.DestinationTableId,
            table.DestinationPath);

        asyncRsps.push_back(Client_->MountTable(
            FromObjectId(table.DestinationTableId),
            options));
    }

    auto rsp = WaitFor(AllSucceeded(asyncRsps));
    ThrowWithClusterNameIfFailed(rsp);
}

void TClusterBackupSession::EnableRestoredReplicas()
{
    std::vector<TFuture<void>> asyncRsps;

    for (const auto& table : Tables_) {
        for (const auto& [replicaId, replicaInfo] : table.Replicas) {
            if (replicaInfo.State == ETableReplicaState::Enabled) {
                YT_LOG_DEBUG("Enabling restored table replica (TableId: %v, TablePath: %v, "
                    "ReplicaId: %v, ReplicaCluster: %v)",
                    table.DestinationTableId,
                    table.DestinationPath,
                    replicaId,
                    replicaInfo.ClusterName);

                asyncRsps.push_back(Client_->AlterTableReplica(
                    replicaInfo.ClonedReplicaId,
                    TAlterTableReplicaOptions{
                        .Enabled = true,
                    }));
            }
        }
    }

    auto rsp = WaitFor(AllSucceeded(asyncRsps));
    ThrowWithClusterNameIfFailed(rsp);
}

auto TClusterBackupSession::GetTables() -> std::vector<TTableInfo*>
{
    std::vector<TTableInfo*> tables;
    tables.reserve(Tables_.size());
    for (auto& tableInfo : Tables_) {
        tables.push_back(&tableInfo);
    }
    return tables;
}

TClusterTag TClusterBackupSession::GetClusterTag() const
{
    return Client_->GetNativeConnection()->GetClusterTag();
}

const TCreateTableBackupOptions& TClusterBackupSession::GetCreateOptions() const
{
    return std::get<TCreateTableBackupOptions>(Options_);
}

const TRestoreTableBackupOptions& TClusterBackupSession::GetRestoreOptions() const
{
    return std::get<TRestoreTableBackupOptions>(Options_);
}

void TClusterBackupSession::ExecuteForAllTables(
    TBuildRequest buildRequest,
    TOnResponse onResponse,
    bool write,
    TMasterReadOptions masterReadOptions)
{
    std::vector<TFuture<TObjectServiceProxy::TRspExecuteBatchPtr>> asyncRsps;
    std::vector<TCellTag> cellTags;

    for (const auto& [cellTag, tableIndexes] : TableIndexesByCellTag_) {
        cellTags.push_back(cellTag);

        auto proxy = write
            ? Client_->CreateObjectServiceWriteProxy(cellTag)
            : Client_->CreateObjectServiceReadProxy(
                masterReadOptions,
                cellTag);
        auto batchReq = proxy.ExecuteBatch();
        for (int tableIndex : tableIndexes) {
            buildRequest(batchReq, Tables_[tableIndex]);
        }

        TPrerequisiteOptions prerequisiteOptions;
        if (cellTag == Client_->GetNativeConnection()->GetPrimaryMasterCellTag()) {
            prerequisiteOptions.PrerequisiteTransactionIds.push_back(Transaction_->GetId());
        } else {
            prerequisiteOptions.PrerequisiteTransactionIds.push_back(
                ExternalizedViaPrimaryCellTransactionId_);
        }
        SetPrerequisites(batchReq, prerequisiteOptions);

        asyncRsps.push_back(batchReq->Invoke());
    }

    auto rspsOrErrors = WaitFor(AllSet(asyncRsps))
        .Value();

    for (int cellTagIndex = 0; cellTagIndex < ssize(cellTags); ++cellTagIndex) {
        const auto& tableIndexes = TableIndexesByCellTag_[cellTags[cellTagIndex]];
        const auto& rspOrError = rspsOrErrors[cellTagIndex];
        ThrowWithClusterNameIfFailed(rspOrError);

        const auto& rsp = rspOrError.ValueOrThrow();
        for (int tableIndex : tableIndexes) {
            onResponse(rsp, &Tables_[tableIndex]);
        }
    }
}

void TClusterBackupSession::ThrowWithClusterNameIfFailed(const TError& error) const
{
    if (!error.IsOK()) {
        THROW_ERROR error << TErrorAttribute("cluster_name", ClusterName_);
    }
}

TTransactionId TClusterBackupSession::GetExternalizedTransactionId(const TTableInfo& table) const
{
    auto primaryMasterCellTag = Client_->GetNativeConnection()->GetPrimaryMasterCellTag();
    if (table.ExternalCellTag == primaryMasterCellTag) {
        return Transaction_->GetId();
    } else {
        return ExternalizedViaPrimaryCellTransactionId_;
    }
}

////////////////////////////////////////////////////////////////////////////////

TBackupSession::TBackupSession(
    TBackupManifestPtr manifest,
    TClientPtr client,
    TCreateOrRestoreTableBackupOptions options,
    NLogging::TLogger logger)
    : Manifest_(std::move(manifest))
    , Client_(std::move(client))
    , Options_(options)
    , Logger(std::move(logger))
{ }

void TBackupSession::RunCreate()
{
    const auto& options = std::get<TCreateTableBackupOptions>(Options_);
    YT_LOG_DEBUG("Generating checkpoint timestamp (Now: %v, Delay: %v)",
        TInstant::Now(),
        options.CheckpointTimestampDelay);
    Timestamp_ = InstantToTimestamp(TInstant::Now() + options.CheckpointTimestampDelay).second;

    YT_LOG_DEBUG("Generated checkpoint timestamp for backup (Timestamp: %v)",
        Timestamp_);

    InitializeAndLockTables(EBackupDirection::Backup);

    YT_LOG_DEBUG("Starting table backups");
    for (const auto& [name, session] : ClusterSessions_) {
        session->StartBackup();
    }

    YT_LOG_DEBUG("Waiting for backup checkpoints");
    for (const auto& [name, session] : ClusterSessions_) {
        session->WaitForCheckpoint();
    }

    YT_LOG_DEBUG("Cloning tables in backup mode");
    for (const auto& [name, session] : ClusterSessions_) {
        session->CloneTables(NCypressClient::ENodeCloneMode::Backup);
    }

    YT_LOG_DEBUG("Finishing backups");
    for (const auto& [name, session] : ClusterSessions_) {
        session->FinishBackups();
    }

    YT_LOG_DEBUG("Validating backup states");
    for (const auto& [name, session] : ClusterSessions_) {
        session->ValidateBackupStates(ETabletBackupState::BackupCompleted);
    }

    YT_LOG_DEBUG("Fetching cloned replica ids");
    for (const auto& [name, session] : ClusterSessions_) {
        session->FetchClonedReplicaIds();
    }

    YT_LOG_DEBUG("Updating upstream replica ids");
    for (const auto& [name, session] : ClusterSessions_) {
        session->UpdateUpstreamReplicaIds();
    }

    YT_LOG_DEBUG("Remembering tablet states");
    for (const auto& [name, session] : ClusterSessions_) {
        session->RememberTabletStates();
    }

    CommitTransactions();
}

void TBackupSession::RunRestore()
{
    const auto& options = std::get<TRestoreTableBackupOptions>(Options_);

    InitializeAndLockTables(EBackupDirection::Restore);

    YT_LOG_DEBUG("Starting table restores");
    for (const auto& [name, session] : ClusterSessions_) {
        session->StartRestore();
    }

    YT_LOG_DEBUG("Cloning tables in restore mode");
    for (const auto& [name, session] : ClusterSessions_) {
        session->CloneTables(NCypressClient::ENodeCloneMode::Restore);
    }

    YT_LOG_DEBUG("Finishing restores");
    for (const auto& [name, session] : ClusterSessions_) {
        session->FinishRestores();
    }

    YT_LOG_DEBUG("Validating backup states");
    for (const auto& [name, session] : ClusterSessions_) {
        session->ValidateBackupStates(ETabletBackupState::None);
    }

    YT_LOG_DEBUG("Fetching cloned replica ids");
    for (const auto& [name, session] : ClusterSessions_) {
        session->FetchClonedReplicaIds();
    }

    YT_LOG_DEBUG("Updating upstream replica ids");
    for (const auto& [name, session] : ClusterSessions_) {
        session->UpdateUpstreamReplicaIds();
    }

    CommitTransactions();

    if (options.Mount) {
        YT_LOG_DEBUG("Mounting restored tables");
        for (const auto& [name, session] : ClusterSessions_) {
            session->MountRestoredTables();
        }
    }

    if (options.EnableReplicas) {
        YT_LOG_DEBUG("Enabling restored replicas");
        for (const auto& [name, session] : ClusterSessions_) {
            session->EnableRestoredReplicas();
        }
    }
}

TClusterBackupSession* TBackupSession::CreateClusterSession(
    const TString& clusterName,
    EBackupDirection direction)
{
    const auto& nativeConnection = Client_->GetNativeConnection();
    auto remoteConnection = GetRemoteConnectionOrThrow(
        nativeConnection,
        clusterName,
        /*syncOnFailure*/ true);
    auto remoteClient = New<TClient>(
        std::move(remoteConnection),
        Client_->GetOptions(),
        /*memoryTracker*/ nullptr);

    auto holder = std::make_unique<TClusterBackupSession>(
        clusterName,
        std::move(remoteClient),
        Options_,
        Timestamp_,
        direction,
        Logger);
    auto* clusterSession = holder.get();
    ClusterSessions_[clusterName] = std::move(holder);
    return clusterSession;
}

void TBackupSession::InitializeAndLockTables(EBackupDirection direction)
{
    for (const auto& [cluster, tables]: Manifest_->Clusters) {
        auto* clusterSession = CreateClusterSession(cluster, direction);
        clusterSession->ValidateBackupsEnabled();
        for (const auto& table : tables) {
            clusterSession->RegisterTable(table);
        }
    }

    MatchReplicatedTablesWithReplicas();

    YT_LOG_DEBUG("Starting backup transactions");
    for (const auto& [name, session] : ClusterSessions_) {
        session->StartTransaction();
    }

    YT_LOG_DEBUG("Locking tables before backup/restore");
    for (const auto& [name, session] : ClusterSessions_) {
        session->LockInputTables();
    }
}

void TBackupSession::MatchReplicatedTablesWithReplicas()
{
    struct TTableInfoWithClusterTag
    {
        TClusterBackupSession::TTableInfo* TableInfo;
        TClusterTag ClusterTag;
    };

    THashMap<TTableReplicaId, TTableInfoWithClusterTag> replicaIdToReplicatedTable;

    for (auto& [clusterName, clusterSession] : ClusterSessions_) {
        for (auto* tableInfo : clusterSession->GetTables()) {
            if (TypeFromId(tableInfo->SourceTableId) != EObjectType::ReplicatedTable) {
                continue;
            }

            for (const auto& [replicaId, replicaInfo] : tableInfo->Replicas) {
                replicaIdToReplicatedTable.emplace(
                    replicaId,
                    TTableInfoWithClusterTag{
                        .TableInfo = tableInfo,
                        .ClusterTag = clusterSession->GetClusterTag(),
                    });
            }
        }
    }

    for (auto& [clusterName, clusterSession] : ClusterSessions_) {
        for (auto* tableInfo : clusterSession->GetTables()) {
            if (!tableInfo->UpstreamReplicaId) {
                continue;
            }

            auto it = replicaIdToReplicatedTable.find(tableInfo->UpstreamReplicaId);
            if (it == replicaIdToReplicatedTable.end()) {
                THROW_ERROR_EXCEPTION("Replica table %v is backed up without corresponding "
                    "replicated table",
                    tableInfo->SourcePath)
                    << TErrorAttribute("cluster_name", clusterName);
            }

            auto* replicatedTableInfo = it->second.TableInfo;
            auto& replicaInfo = replicatedTableInfo->Replicas[tableInfo->UpstreamReplicaId];

            replicatedTableInfo->BackupableReplicas.push_back({
                .ReplicaId = replicaInfo.Id,
                .Mode = replicaInfo.Mode,
                .ReplicaPath = tableInfo->DestinationPath,
            });

            tableInfo->ClockClusterTag = it->second.ClusterTag;
            tableInfo->ReplicaMode = replicaInfo.Mode;
            tableInfo->UpstreamReplica = &replicaInfo;

            replicaInfo.ClonedReplicaPath = tableInfo->DestinationPath;
        }
    }

    for (auto& [clusterName, clusterSession] : ClusterSessions_) {
        for (auto* tableInfo : clusterSession->GetTables()) {
            if (TypeFromId(tableInfo->SourceTableId) == EObjectType::ReplicatedTable &&
                tableInfo->BackupableReplicas.empty())
            {
                THROW_ERROR_EXCEPTION("No replicas of replicated table %v are backed up",
                    tableInfo->SourcePath)
                    << TErrorAttribute("cluster_name", clusterName);
            }
        }
    }
}

void TBackupSession::CommitTransactions()
{
    YT_LOG_DEBUG("Committing backup transactions");
    for (const auto& [name, session] : ClusterSessions_) {
        session->CommitTransaction();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative
