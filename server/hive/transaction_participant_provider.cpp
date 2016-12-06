#include "transaction_participant_provider.h"

#include <yt/ytlib/api/client.h>
#include <yt/ytlib/api/connection.h>
#include <yt/ytlib/api/native_connection.h>
#include <yt/ytlib/api/native_transaction_participant.h>

#include <yt/ytlib/object_client/helpers.h>

#include <yt/ytlib/hive/cluster_directory.h>

namespace NYT {
namespace NHiveServer {

using namespace NApi;
using namespace NHiveClient;
using namespace NObjectClient;

////////////////////////////////////////////////////////////////////////////////

class TCellDirectoryTransactionParticipantProvider
    : public ITransactionParticipantProvider
{
public:
    explicit TCellDirectoryTransactionParticipantProvider(
        TCellTag cellTag,
        TCellDirectoryPtr cellDirectory)
        : CellTag_(cellTag)
        , CellDirectory_(std::move(cellDirectory))
    { }

    virtual ITransactionParticipantPtr TryCreate(
        const TCellId& cellId,
        const TTransactionParticipantOptions& options) override
    {
        if (CellTagFromId(cellId) != CellTag_) {
            return nullptr;
        }
        return CreateNativeTransactionParticipant(CellDirectory_, cellId, options);
    }

private:
    const TCellTag CellTag_;
    const TCellDirectoryPtr CellDirectory_;

};

ITransactionParticipantProviderPtr CreateTransactionParticipantProvider(
    TCellTag cellTag,
    TCellDirectoryPtr cellDirectory)
{
    return New<TCellDirectoryTransactionParticipantProvider>(
        cellTag,
        std::move(cellDirectory));
}

ITransactionParticipantProviderPtr CreateTransactionParticipantProvider(
    INativeConnectionPtr connection)
{
    return CreateTransactionParticipantProvider(
        connection->GetCellTag(),
        connection->GetCellDirectory());
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
    return New<TClusterDirectoryTransactionParticipantProvider>(std::move(clusterDirectory));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NHiveServer
} // namespace NYT
