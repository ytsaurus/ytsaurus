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

class TClusterBackupSession
{
public:
    struct TTableInfo
    {
        NYPath::TYPath SourcePath;
        NTableClient::TTableId SourceTableId;
        NObjectClient::TCellTag ExternalCellTag;
        NYTree::IAttributeDictionaryPtr Attributes;

        NYPath::TYPath DestinationPath;
        NTableClient::TTableId DestinationTableId;
    };

public:
    TClusterBackupSession(
        TString clusterName,
        TClientPtr client,
        TCreateOrRestoreTableBackupOptions options,
        NTransactionClient::TTimestamp timestamp,
        NLogging::TLogger logger);

    ~TClusterBackupSession();

    void RegisterTable(const TTableBackupManifestPtr& manifest);

    void StartTransaction(TStringBuf title);

    void LockInputTables();

    void SetCheckpoint();

    void WaitForCheckpoint();

    void CloneTables(NCypressClient::ENodeCloneMode nodeCloneMode);

    void FinishBackups();

    void FinishRestores();

    void ValidateBackupStates(NTabletClient::ETabletBackupState expectedState);

    void CommitTransaction();

private:
    const TString ClusterName_;
    const TClientPtr Client_;
    const TCreateOrRestoreTableBackupOptions Options_;
    const NTransactionClient::TTimestamp Timestamp_;
    const NLogging::TLogger Logger;

    NApi::NNative::ITransactionPtr Transaction_;

    std::vector<TTableInfo> Tables_;
    THashMap<NObjectClient::TCellTag, std::vector<int>> TableIndexesByCellTag_;
    THashSet<NTableClient::TTableId> SourceTableIds_;
    THashSet<NObjectClient::TCellTag> CellTags_;

    using TBuildRequest = std::function<
        void(
            const NObjectClient::TObjectServiceProxy::TReqExecuteBatchPtr& req,
            const TTableInfo& table)>;
    using TOnResponse = std::function<
        void(const NObjectClient::TObjectServiceProxy::TRspExecuteBatchPtr& rsp,
            TTableInfo* table)>;

    const TCreateTableBackupOptions& GetCreateOptions() const;

    const TRestoreTableBackupOptions& GetRestoreOptions() const;

    void ExecuteForAllTables(
        TBuildRequest buildRequest,
        TOnResponse onResponse,
        bool write,
        TMasterReadOptions masterReadOptions = {});

    void ThrowWithClusterNameIfFailed(const TError& error) const;
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

    TClusterBackupSession* CreateClusterSession(const TString& clusterName);

    void InitializeAndLockTables(TStringBuf transactionTitle);

    void CommitTransactions();
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative
