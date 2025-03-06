#include "helpers.h"

#include "lease_manager.h"

#include <yt/yt/server/lib/hive/hive_manager.h>

#include <yt/yt/ytlib/transaction_client/transaction_service_proxy.h>

#include <yt/yt/client/object_client/helpers.h>

namespace NYT::NLeaseServer {

using namespace NHiveServer;
using namespace NHydra;
using namespace NObjectClient;
using namespace NRpc;
using namespace NTransactionClient;

////////////////////////////////////////////////////////////////////////////////

TFuture<void> IssueLeasesForCell(
    const std::vector<TTransactionId>& prerequisiteTransactionIds,
    const ILeaseManagerPtr& leaseManager,
    const IHiveManagerPtr& hiveManager,
    TCellId selfCellId,
    bool synWithAllLeaseTransactionCoordinators,
    TCallback<TCellId(TCellTag)> getMasterCellId,
    TCallback<IChannelPtr(TCellTag)> findMasterChannel)
{
    THashMap<TCellTag, std::vector<TLeaseId>> coordinatorCellTagToTransactionIds;
    THashSet<TCellTag> coordinatorCellTagsToSyncWith;
    coordinatorCellTagToTransactionIds.reserve(prerequisiteTransactionIds.size());
    coordinatorCellTagsToSyncWith.reserve(prerequisiteTransactionIds.size());

    for (auto transactionId : prerequisiteTransactionIds) {
        auto coordinatorCellTag = CellTagFromId(transactionId);
        if (!leaseManager->FindLease(transactionId)) {
            coordinatorCellTagToTransactionIds[coordinatorCellTag].push_back(transactionId);
        } else if (synWithAllLeaseTransactionCoordinators) {
            coordinatorCellTagsToSyncWith.insert(coordinatorCellTag);
        }
    }

    std::vector<TFuture<void>> futures;
    futures.reserve(coordinatorCellTagToTransactionIds.size() + coordinatorCellTagsToSyncWith.size());
    for (const auto& [coordinatorCellTag, transactionIds] : coordinatorCellTagToTransactionIds) {
        auto coordinatorCellId = getMasterCellId(coordinatorCellTag);
        auto masterChannel = findMasterChannel(coordinatorCellTag);
        THROW_ERROR_EXCEPTION_UNLESS(
            masterChannel,
            "Failed to issue leases for prerequisite transactions: unknown master cell tag %v",
            coordinatorCellTag);

        TTransactionServiceProxy proxy(std::move(masterChannel));
        auto req = proxy.IssueLeases();
        ToProto(req->mutable_transaction_ids(), transactionIds);
        ToProto(req->mutable_cell_id(), selfCellId);

        auto coordinatorSyncFuture = req->Invoke().AsVoid().Apply(BIND([=] {
            return hiveManager->SyncWith(coordinatorCellId, /*enableBatching*/ true);
        }));
        futures.push_back(std::move(coordinatorSyncFuture));
        coordinatorCellTagsToSyncWith.erase(coordinatorCellTag);
    }

    YT_VERIFY(coordinatorCellTagsToSyncWith.empty() || synWithAllLeaseTransactionCoordinators);

    for (auto cellTag : coordinatorCellTagsToSyncWith) {
        auto coordinatorCellId = getMasterCellId(cellTag);
        futures.push_back(hiveManager->SyncWith(coordinatorCellId, /*enableBatching*/ true));
    }

    return AllSucceeded(std::move(futures));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLeaseServer
