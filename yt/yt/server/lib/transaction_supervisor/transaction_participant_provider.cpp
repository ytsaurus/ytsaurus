#include "transaction_participant_provider.h"
#include "private.h"

#include <yt/yt/client/api/client.h>
#include <yt/yt/client/api/connection.h>

#include <yt/yt/ytlib/api/native/connection.h>
#include <yt/yt/ytlib/api/native/transaction_participant.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/ytlib/hive/cluster_directory.h>
#include <yt/yt/ytlib/hive/cell_directory_synchronizer.h>

namespace NYT::NTransactionSupervisor {

using namespace NApi;
using namespace NHiveClient;
using namespace NObjectClient;
using namespace NTransactionClient;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = TransactionSupervisorLogger;

////////////////////////////////////////////////////////////////////////////////

class TCellDirectoryTransactionParticipantProvider
    : public ITransactionParticipantProvider
{
public:
    TCellDirectoryTransactionParticipantProvider(
        ICellDirectoryPtr cellDirectory,
        ICellDirectorySynchronizerPtr cellDirectorySynchronizer,
        ITimestampProviderPtr timestampProvider,
        const TCellTagList& cellTags)
        : CellDirectory_(std::move(cellDirectory))
        , CellDirectorySynchronizer_(std::move(cellDirectorySynchronizer))
        , TimestampProvider_(std::move(timestampProvider))
        , CellTags_(cellTags)
    { }

    ITransactionParticipantPtr TryCreate(
        TCellId cellId,
        const TTransactionParticipantOptions& options) override
    {
        if (TypeFromId(cellId) != EObjectType::ChaosCell &&
            std::find(CellTags_.begin(), CellTags_.end(), CellTagFromId(cellId)) == CellTags_.end())
        {
            // NB: This is necessary for replicated tables. If cell is foreign, then next participant
            // provider is used, which is cluster directory participant provider.
            return nullptr;
        }

        YT_LOG_DEBUG("Transaction participant is provided by cell directory (CellId: %v)",
            cellId);

        return NNative::CreateTransactionParticipant(
            CellDirectory_,
            CellDirectorySynchronizer_,
            TimestampProvider_,
            nullptr,
            cellId,
            options);
    }

private:
    const ICellDirectoryPtr CellDirectory_;
    const ICellDirectorySynchronizerPtr CellDirectorySynchronizer_;
    const ITimestampProviderPtr TimestampProvider_;
    const TCellTagList CellTags_;
};

ITransactionParticipantProviderPtr CreateTransactionParticipantProvider(
    ICellDirectoryPtr cellDirectory,
    ICellDirectorySynchronizerPtr cellDirectorySynchronizer,
    ITimestampProviderPtr timestampProvider,
    const TCellTagList& cellTags)
{
    return New<TCellDirectoryTransactionParticipantProvider>(
        std::move(cellDirectory),
        std::move(cellDirectorySynchronizer),
        std::move(timestampProvider),
        cellTags);
}

ITransactionParticipantProviderPtr CreateTransactionParticipantProvider(
    NNative::IConnectionPtr connection)
{
    // Ensure cell directory sync.
    connection->GetCellDirectorySynchronizer()->Start();
    return CreateTransactionParticipantProvider(
        connection->GetCellDirectory(),
        connection->GetCellDirectorySynchronizer(),
        connection->GetTimestampProvider(),
        {connection->GetPrimaryMasterCellTag()});
}

////////////////////////////////////////////////////////////////////////////////

class TClusterDirectoryTransactionParticipantProvider
    : public ITransactionParticipantProvider
{
public:
    explicit TClusterDirectoryTransactionParticipantProvider(TClusterDirectoryPtr clusterDirectory)
        : ClusterDirectory_(std::move(clusterDirectory))
    { }

    ITransactionParticipantPtr TryCreate(
        TCellId cellId,
        const TTransactionParticipantOptions& options) override
    {
        auto connection = ClusterDirectory_->FindConnection(CellTagFromId(cellId));
        if (!connection) {
            return nullptr;
        }

        YT_LOG_DEBUG("Transaction participant is provided by remote connection (CellId: %v, ClusterId: %v)",
            cellId,
            connection->GetClusterId());

        return connection->CreateTransactionParticipant(cellId, options);
    }

private:
    const TClusterDirectoryPtr ClusterDirectory_;
};

ITransactionParticipantProviderPtr CreateTransactionParticipantProvider(
    TClusterDirectoryPtr clusterDirectory)
{
    return New<TClusterDirectoryTransactionParticipantProvider>(
        std::move(clusterDirectory));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTransactionSupervisor
