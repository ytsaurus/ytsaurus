#include "backup_session.h"

#include "client.h"
#include "transaction.h"

#include <yt/yt/ytlib/cypress_client/rpc_helpers.h>

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

TClusterBackupSession::TClusterBackupSession(
    TString clusterName,
    TClientPtr client,
    TCreateOrRestoreTableBackupOptions options,
    TTimestamp timestamp,
    NLogging::TLogger logger)
    : ClusterName_(std::move(clusterName))
    , Client_(std::move(client))
    , Options_(options)
    , Timestamp_(timestamp)
    , Logger(logger
        .WithTag("SessionId: %v", TGuid::Create())
        .WithTag("Cluster: %v", ClusterName_))
{ }

TClusterBackupSession::~TClusterBackupSession()
{
    if (Transaction_) {
        YT_LOG_DEBUG("Aborting backup transaction due to session failure");
        Transaction_->Abort();
    }
}

void TClusterBackupSession::RegisterTable(const TTableBackupManifestPtr& manifest)
{
    TTableInfo tableInfo;
    tableInfo.SourcePath = manifest->SourcePath;
    tableInfo.DestinationPath = manifest->DestinationPath;

    tableInfo.Attributes = Client_->ResolveExternalTable(
        tableInfo.SourcePath,
        &tableInfo.SourceTableId,
        &tableInfo.ExternalCellTag,
        {"sorted", "upstream_replica_id", "replicas", "dynamic", "commit_ordering"});

    auto sorted = tableInfo.Attributes->Get<bool>("sorted");
    auto dynamic = tableInfo.Attributes->Get<bool>("dynamic");
    auto commitOrdering = tableInfo.Attributes->Get<ECommitOrdering>("commit_ordering");
    auto upstreamReplicaId = tableInfo.Attributes->Get<TTableReplicaId>("upstream_replica_id");
    auto type = TypeFromId(tableInfo.SourceTableId);

    try {
        if (!SourceTableIds_.insert(tableInfo.SourceTableId).second) {
            THROW_ERROR_EXCEPTION("Duplicate table %Qv in backup manifest",
                tableInfo.SourcePath);
        }

        if (!dynamic) {
            THROW_ERROR_EXCEPTION("Table %Qv is not dynamic",
                tableInfo.SourcePath);
        }

        if (type == EObjectType::ReplicatedTable) {
            THROW_ERROR_EXCEPTION("Table %Qv is replicated",
                tableInfo.SourcePath);
        }

        if (type == EObjectType::ReplicationLogTable) {
            THROW_ERROR_EXCEPTION("Table %Qv is a replication log",
                tableInfo.SourcePath);
        }

        if (upstreamReplicaId) {
            THROW_ERROR_EXCEPTION("Table %Qv is a replica table",
                tableInfo.SourcePath);
        }

        if (sorted) {
            if (commitOrdering != ECommitOrdering::Weak) {
                THROW_ERROR_EXCEPTION("Sorted table %Qv has unsupported commit ordering %Qlv",
                    tableInfo.SourcePath,
                    commitOrdering);
            }
        } else {
            if (commitOrdering != ECommitOrdering::Strong) {
                THROW_ERROR_EXCEPTION("Ordered table %Qv has unsupported commit ordering %Qlv",
                    tableInfo.SourcePath,
                    commitOrdering);
            }
        }

        if (CellTagFromId(tableInfo.SourceTableId) !=
            Client_->GetNativeConnection()->GetPrimaryMasterCellTag())
        {
            THROW_ERROR_EXCEPTION("Table %Qv is beyond the portal",
                tableInfo.SourcePath);
        }
    } catch (const TErrorException& e) {
        ThrowWithClusterNameIfFailed(e);
    }

    CellTags_.insert(CellTagFromId(tableInfo.SourceTableId));
    CellTags_.insert(tableInfo.ExternalCellTag);

    TableIndexesByCellTag_[tableInfo.ExternalCellTag].push_back(ssize(Tables_));
    Tables_.push_back(std::move(tableInfo));
}

void TClusterBackupSession::StartTransaction(TStringBuf title)
{
    auto transactionAttributes = CreateEphemeralAttributes();
    transactionAttributes->Set(
        "title",
        title);
    TNativeTransactionStartOptions options;
    options.Attributes = std::move(transactionAttributes);
    options.ReplicateToMasterCellTags = TCellTagList(CellTags_.begin(), CellTags_.end());
    auto asyncTransaction = Client_->StartNativeTransaction(
        NTransactionClient::ETransactionType::Master,
        options);
    Transaction_ = WaitFor(asyncTransaction)
        .ValueOrThrow();
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

void TClusterBackupSession::SetCheckpoint()
{
    auto buildRequest = [&] (const auto& batchReq, const TTableInfo& table) {
        auto req = TTableYPathProxy::SetBackupCheckpoint(FromObjectId(table.SourceTableId));
        req->set_timestamp(Timestamp_);
        SetTransactionId(req, Transaction_->GetId());
        batchReq->AddRequest(req, ToString(table.SourceTableId));
    };

    auto onResponse = [&] (const auto& batchRsp, TTableInfo* table) {
        const auto& rsp = batchRsp->template GetResponse<TTableYPathProxy::TRspSetBackupCheckpoint>(
            ToString(table->SourceTableId));
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

        auto req = TTableYPathProxy::CheckBackupCheckpoint(FromObjectId(table.SourceTableId));
        SetTransactionId(req, Transaction_->GetId());
        batchReq->AddRequest(req, ToString(table.SourceTableId));
    };

    auto onResponse = [&] (const auto& batchRsp, TTableInfo* table) {
        if (!unconfirmedTables.contains(table)) {
            return;
        }

        const auto& rspOrError = batchRsp->template GetResponse<TTableYPathProxy::TRspCheckBackupCheckpoint>(
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
    if (nodeCloneMode == ENodeCloneMode::Backup) {
        force = GetCreateOptions().Force;
    } else {
        force = GetRestoreOptions().Force;
    }

    // TODO(ifsmirnov): this doesn't work for tables beyond the portals.
    auto proxy = Client_->CreateWriteProxy<TObjectServiceProxy>();
    auto batchReq = proxy->ExecuteBatch();

    for (const auto& table : Tables_) {
        auto req = TCypressYPathProxy::Copy(table.DestinationPath);
        req->set_mode(static_cast<int>(nodeCloneMode));
        req->set_force(force);
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
        SetTransactionId(req, Transaction_->GetId());
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
        SetTransactionId(req, Transaction_->GetId());
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
        SetTransactionId(req, Transaction_->GetId());
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

void TClusterBackupSession::CommitTransaction()
{
    auto rsp = WaitFor(Transaction_->Commit());
    ThrowWithClusterNameIfFailed(rsp);
    Transaction_ = nullptr;
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
            ? Client_->CreateWriteProxy<TObjectServiceProxy>(cellTag)
            : Client_->CreateReadProxy<TObjectServiceProxy>(
                masterReadOptions,
                cellTag);
        auto batchReq = proxy->ExecuteBatch();
        for (int tableIndex : tableIndexes) {
            buildRequest(batchReq, Tables_[tableIndex]);
        }

        TPrerequisiteOptions prerequisiteOptions;
        prerequisiteOptions.PrerequisiteTransactionIds.push_back(Transaction_->GetId());
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
        YT_VERIFY(rsp->GetResponseCount() <= ssize(tableIndexes));
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

    YT_LOG_DEBUG("Generated checkpoint timestamp for backup (Timestamp: %llx)",
        Timestamp_);

    InitializeAndLockTables("Create backup");

    YT_LOG_DEBUG("Setting backup checkpoints");
    for (const auto& [name, session] : ClusterSessions_) {
        session->SetCheckpoint();
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

    CommitTransactions();
}

void TBackupSession::RunRestore()
{
    InitializeAndLockTables("Restore backup");

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

    CommitTransactions();
}

TClusterBackupSession* TBackupSession::CreateClusterSession(const TString& clusterName)
{
    const auto& nativeConnection = Client_->GetNativeConnection();
    auto remoteConnection = GetRemoteConnectionOrThrow(
        nativeConnection,
        clusterName,
        /*syncOnFailure*/ true);
    auto remoteClient = New<TClient>(
        std::move(remoteConnection),
        Client_->GetOptions());

    auto holder = std::make_unique<TClusterBackupSession>(
        clusterName,
        std::move(remoteClient),
        Options_,
        Timestamp_,
        Logger);
    auto* clusterSession = holder.get();
    ClusterSessions_[clusterName] = std::move(holder);
    return clusterSession;
}

void TBackupSession::InitializeAndLockTables(TStringBuf transactionTitle)
{
    for (const auto& [cluster, tables]: Manifest_->Clusters) {
        auto* clusterSession = CreateClusterSession(cluster);
        for (const auto& table : tables) {
            clusterSession->RegisterTable(table);
        }
    }

    YT_LOG_DEBUG("Starting backup transactions");
    for (const auto& [name, session] : ClusterSessions_) {
        session->StartTransaction(transactionTitle);
    }

    YT_LOG_DEBUG("Locking tables before backup/restore");
    for (const auto& [name, session] : ClusterSessions_) {
        session->LockInputTables();
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
