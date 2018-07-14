#include "transaction_participant_provider.h"

#include <yt/ytlib/api/client.h>
#include <yt/ytlib/api/connection.h>

#include <yt/ytlib/api/native/connection.h>
#include <yt/ytlib/api/native/transaction_participant.h>

#include <yt/ytlib/object_client/helpers.h>

#include <yt/ytlib/hive/cluster_directory.h>
#include <yt/ytlib/hive/cell_directory_synchronizer.h>

namespace NYT {
namespace NHiveServer {

using namespace NApi;
using namespace NHiveClient;
using namespace NObjectClient;
using namespace NTransactionClient;

////////////////////////////////////////////////////////////////////////////////

class TCellDirectoryTransactionParticipantProvider
    : public ITransactionParticipantProvider
{
public:
    TCellDirectoryTransactionParticipantProvider(
        TCellDirectoryPtr cellDirectory,
        ITimestampProviderPtr timestampProvider,
        TCellTag cellTag)
        : CellDirectory_(std::move(cellDirectory))
        , TimestampProvider_(std::move(timestampProvider))
        , CellTag_(cellTag)
    { }

    virtual ITransactionParticipantPtr TryCreate(
        const TCellId& cellId,
        const TTransactionParticipantOptions& options) override
    {
        if (CellTagFromId(cellId) != CellTag_) {
            return nullptr;
        }
        return NNative::CreateTransactionParticipant(
            CellDirectory_,
            nullptr,
            TimestampProvider_,
            nullptr,
            cellId,
            options);
    }

private:
    const TCellDirectoryPtr CellDirectory_;
    const ITimestampProviderPtr TimestampProvider_;
    const TCellTag CellTag_;
};

ITransactionParticipantProviderPtr CreateTransactionParticipantProvider(
    TCellDirectoryPtr cellDirectory,
    ITimestampProviderPtr timestampProvider,
    TCellTag cellTag)
{
    return New<TCellDirectoryTransactionParticipantProvider>(
        std::move(cellDirectory),
        std::move(timestampProvider),
        cellTag);
}

ITransactionParticipantProviderPtr CreateTransactionParticipantProvider(
    NNative::IConnectionPtr connection)
{
    // Ensure cell directory sync.
    connection->GetCellDirectorySynchronizer()->Start();
    return CreateTransactionParticipantProvider(
        connection->GetCellDirectory(),
        connection->GetTimestampProvider(),
        connection->GetCellTag());
}

////////////////////////////////////////////////////////////////////////////////

class TClusterDirectoryTransactionParticipantProvider
    : public ITransactionParticipantProvider
{
public:
    explicit TClusterDirectoryTransactionParticipantProvider(TClusterDirectoryPtr clusterDirectory)
        : ClusterDirectory_(std::move(clusterDirectory))
    { }

    virtual ITransactionParticipantPtr TryCreate(
        const TCellId& cellId,
        const TTransactionParticipantOptions& options) override
    {
        auto connection = ClusterDirectory_->FindConnection(CellTagFromId(cellId));
        if (!connection) {
            return nullptr;
        }
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

} // namespace NHiveServer
} // namespace NYT
