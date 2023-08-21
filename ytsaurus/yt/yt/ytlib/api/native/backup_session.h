#pragma once

#include "client_impl.h"

#include <yt/yt/ytlib/tablet_client/backup.h>

#include <library/cpp/yt/yson_string/string.h>

namespace NYT::NApi::NNative {

////////////////////////////////////////////////////////////////////////////////

using TCreateOrRestoreTableBackupOptions = std::variant<
    TCreateTableBackupOptions,
    TRestoreTableBackupOptions
>;

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EBackupDirection,
    (Backup)
    (Restore)
);

////////////////////////////////////////////////////////////////////////////////

class TClusterBackupSession
{
public:
    struct TTableReplicaInfo
    {
        NTabletClient::TTableReplicaId Id;
        TString ClusterName;
        NTabletClient::ETableReplicaMode Mode;
        NTabletClient::ETableReplicaState State;
        TString ReplicaPath;
        // Path to the corresponding backed up or restored table.
        TString ClonedReplicaPath;
        NTabletClient::TTableReplicaId ClonedReplicaId;
    };

    struct TTableInfo
    {
        NYPath::TYPath SourcePath;
        NTableClient::TTableId SourceTableId;
        NObjectClient::TCellTag ExternalCellTag;
        NYTree::IAttributeDictionaryPtr Attributes;

        bool Sorted = false;
        bool Replicated = false;
        NTabletClient::ETabletState TabletState;
        NTransactionClient::ECommitOrdering CommitOrdering{};
        NTabletClient::EOrderedTableBackupMode OrderedTableBackupMode;

        NYPath::TYPath DestinationPath;
        NTableClient::TTableId DestinationTableId;

        // Specific for replica tables.
        NTabletClient::TTableReplicaId UpstreamReplicaId;
        const TTableReplicaInfo* UpstreamReplica = nullptr;
        TClusterTag ClockClusterTag = TClusterTag{};
        NTabletClient::ETableReplicaMode ReplicaMode;

        // Specific for replicated tables.
        THashMap<NTabletClient::TTableReplicaId, TTableReplicaInfo> Replicas;
        std::vector<NTabletClient::TTableReplicaBackupDescriptor> BackupableReplicas;
    };

public:
    TClusterBackupSession(
        TString clusterName,
        TClientPtr client,
        TCreateOrRestoreTableBackupOptions options,
        NTransactionClient::TTimestamp timestamp,
        EBackupDirection direction,
        NLogging::TLogger logger);

    ~TClusterBackupSession();

    void ValidateBackupsEnabled();

    void RegisterTable(const TTableBackupManifestPtr& manifest);

    void StartTransaction();

    void LockInputTables();

    void StartBackup();

    void StartRestore();

    void WaitForCheckpoint();

    void CloneTables(NCypressClient::ENodeCloneMode nodeCloneMode);

    void FinishBackups();

    void FinishRestores();

    void ValidateBackupStates(NTabletClient::ETabletBackupState expectedState);

    void FetchClonedReplicaIds();

    void UpdateUpstreamReplicaIds();

    void RememberTabletStates();

    void CommitTransaction();

    void MountRestoredTables();

    void EnableRestoredReplicas();

    std::vector<TTableInfo*> GetTables();

    TClusterTag GetClusterTag() const;

private:
    const TString ClusterName_;
    const TClientPtr Client_;
    const TCreateOrRestoreTableBackupOptions Options_;
    const NTransactionClient::TTimestamp Timestamp_;
    const EBackupDirection Direction_;
    const NLogging::TLogger Logger;

    NApi::NNative::ITransactionPtr Transaction_;
    // Externalized transaction should be used for direct requests to external cells.
    NTransactionClient::TTransactionId ExternalizedViaPrimaryCellTransactionId_;

    std::vector<TTableInfo> Tables_;
    THashMap<NObjectClient::TCellTag, std::vector<int>> TableIndexesByCellTag_;
    THashSet<NTableClient::TTableId> SourceTableIds_;
    THashSet<NObjectClient::TCellTag> CellTags_;

    using TBuildRequest = std::function<
        void(
            const NObjectClient::TObjectServiceProxy::TReqExecuteBatchPtr& req,
            const TTableInfo& table)>;
    using TOnResponse = std::function<
        void(
            const NObjectClient::TObjectServiceProxy::TRspExecuteBatchPtr& rsp,
            TTableInfo* table)>;

    const TCreateTableBackupOptions& GetCreateOptions() const;

    const TRestoreTableBackupOptions& GetRestoreOptions() const;

    void ExecuteForAllTables(
        TBuildRequest buildRequest,
        TOnResponse onResponse,
        bool write,
        TMasterReadOptions masterReadOptions = {});

    void ThrowWithClusterNameIfFailed(const TError& error) const;

    NTransactionClient::TTransactionId GetExternalizedTransactionId(const TTableInfo& table) const;
};

////////////////////////////////////////////////////////////////////////////////

class TBackupSession
{
public:
    TBackupSession(
        TBackupManifestPtr manifest,
        TClientPtr client,
        TCreateOrRestoreTableBackupOptions options,
        NLogging::TLogger logger);

    void RunCreate();

    void RunRestore();

private:
    const TBackupManifestPtr Manifest_;
    const TClientPtr Client_;
    const TCreateOrRestoreTableBackupOptions Options_;
    const NLogging::TLogger Logger;

    NTransactionClient::TTimestamp Timestamp_ = NTransactionClient::NullTimestamp;

    THashMap<TString, std::unique_ptr<TClusterBackupSession>> ClusterSessions_;

    TClusterBackupSession* CreateClusterSession(
        const TString& clusterName,
        EBackupDirection direction);

    void InitializeAndLockTables(EBackupDirection direction);

    void MatchReplicatedTablesWithReplicas();

    void CommitTransactions();
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative
