#include "bulk_insert_locking.h"
#include "private.h"

#include <yt/yt/ytlib/cypress_client/rpc_helpers.h>

#include <yt/yt/ytlib/object_client/object_service_proxy.h>

#include <yt/yt/ytlib/table_client/table_ypath_proxy.h>

#include <yt/yt/ytlib/tablet_client/proto/bulk_insert_locking.pb.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/client/transaction_client/timestamp_provider.h>

namespace NYT {

using namespace NApi;
using namespace NConcurrency;
using namespace NCypressClient;
using namespace NLogging;
using namespace NObjectClient;
using namespace NTableClient;
using namespace NYPath;

////////////////////////////////////////////////////////////////////////////////

namespace NTabletClient {

////////////////////////////////////////////////////////////////////////////////

void LockDynamicTables(
    THashMap<TCellTag, std::vector<TLockableDynamicTable>> lockableDynamicTables,
    const NNative::IConnectionPtr& connection,
    const TExponentialBackoffOptions& config,
    const TLogger& Logger)
{
    YT_LOG_INFO("Locking output dynamic tables");

    const auto& timestampProvider = connection->GetTimestampProvider();
    auto currentTimestampOrError = WaitFor(timestampProvider->GenerateTimestamps());
    THROW_ERROR_EXCEPTION_IF_FAILED(currentTimestampOrError, "Error generating timestamp to lock output dynamic tables");
    auto currentTimestamp = currentTimestampOrError.Value();

    std::vector<TFuture<TObjectServiceProxy::TRspExecuteBatchPtr>> asyncResults;
    std::vector<TCellTag> externalCellTags;
    for (const auto& [externalCellTag, tables] : lockableDynamicTables) {
        TObjectServiceProxy proxy(
            connection,
            EMasterChannelKind::Leader,
            externalCellTag,
            /*stickyGroupSizeCache*/ nullptr);
        auto batchReq = proxy.ExecuteBatch();

        for (const auto& table : tables) {
            auto req = TTableYPathProxy::LockDynamicTable(FromObjectId(table.TableId));
            req->set_timestamp(currentTimestamp);
            AddCellTagToSyncWith(req, table.TableId);
            SetTransactionId(req, table.ExternalTransactionId);
            GenerateMutationId(req);
            batchReq->AddRequest(req);
        }

        asyncResults.push_back(batchReq->Invoke());
        externalCellTags.push_back(externalCellTag);
    }

    auto combinedResultOrError = WaitFor(AllSet(asyncResults));
    THROW_ERROR_EXCEPTION_IF_FAILED(combinedResultOrError, "Error locking output dynamic tables");
    auto& combinedResult = combinedResultOrError.Value();

    for (const auto& batchRspOrError : combinedResult) {
        THROW_ERROR_EXCEPTION_IF_FAILED(GetCumulativeError(batchRspOrError), "Error locking output dynamic tables");
    }

    YT_LOG_INFO("Waiting for dynamic tables lock to complete");

    TBackoffStrategy backoff(config);

    std::vector<TError> innerErrors;
    do {
        asyncResults.clear();
        externalCellTags.clear();
        innerErrors.clear();

        for (const auto& [externalCellTag, tables] : lockableDynamicTables) {
            TObjectServiceProxy proxy(
                connection,
                EMasterChannelKind::Follower,
                externalCellTag,
                /*stickyGroupSizeCache*/ nullptr);

            auto batchReq = proxy.ExecuteBatch();
            for (const auto& table : tables) {
                auto req = TTableYPathProxy::CheckDynamicTableLock(FromObjectId(table.TableId));
                AddCellTagToSyncWith(req, table.TableId);
                SetTransactionId(req, table.ExternalTransactionId);
                batchReq->AddRequest(req);
            }

            asyncResults.push_back(batchReq->Invoke());
            externalCellTags.push_back(externalCellTag);
        }

        auto combinedResultOrError = WaitFor(AllSet(asyncResults));
        if (!combinedResultOrError.IsOK()) {
            innerErrors.push_back(combinedResultOrError);
            continue;
        }
        auto& combinedResult = combinedResultOrError.Value();

        for (int cellIndex = 0; cellIndex < ssize(externalCellTags); ++cellIndex) {
            auto& batchRspOrError = combinedResult[cellIndex];
            auto cumulativeError = GetCumulativeError(batchRspOrError);
            if (!cumulativeError.IsOK()) {
                if (cumulativeError.FindMatching(NTransactionClient::EErrorCode::NoSuchTransaction)) {
                    cumulativeError.ThrowOnError();
                }
                innerErrors.push_back(cumulativeError);
                YT_LOG_DEBUG(cumulativeError, "Error while checking dynamic table lock");
                continue;
            }

            const auto& batchRsp = batchRspOrError.Value();
            auto checkLockRspsOrError = batchRsp->GetResponses<TTableYPathProxy::TRspCheckDynamicTableLock>();

            auto& tables = lockableDynamicTables[externalCellTags[cellIndex]];
            std::vector<TLockableDynamicTable> pendingTables;

            for (int index = 0; index < ssize(tables); ++index) {
                const auto& rspOrError = checkLockRspsOrError[index];

                if (!rspOrError.IsOK()) {
                    if (rspOrError.FindMatching(NTransactionClient::EErrorCode::NoSuchTransaction)) {
                        rspOrError.ThrowOnError();
                    }
                    innerErrors.push_back(rspOrError);
                    YT_LOG_DEBUG(rspOrError, "Error while checking dynamic table lock");
                }

                if (!rspOrError.IsOK() || !rspOrError.Value()->confirmed()) {
                    pendingTables.push_back(std::move(tables[index]));
                }
            }

            if (tables = std::move(pendingTables); tables.empty()) {
                lockableDynamicTables.erase(externalCellTags[cellIndex]);
            }
        }

        if (lockableDynamicTables.empty()) {
            break;
        }

        TDelayedExecutor::WaitForDuration(backoff.GetBackoff());
    } while (backoff.Next());

    if (!innerErrors.empty()) {
        THROW_ERROR_EXCEPTION("Could not lock output dynamic tables")
            << std::move(innerErrors);
    }

    YT_LOG_INFO("Dynamic tables locking completed");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletClient

////////////////////////////////////////////////////////////////////////////////

void ToProto(
    NTabletClient::NProto::TExternalCellTagToTableIds* protoExternalCellTagToTableIds,
    const std::pair<TCellTag, std::vector<TTableId>>& externalCellTagToTableIds)
{
    protoExternalCellTagToTableIds->set_external_cell_tag(externalCellTagToTableIds.first.Underlying());
    ToProto(protoExternalCellTagToTableIds->mutable_table_ids(), externalCellTagToTableIds.second);
}

void FromProto(
    std::pair<TCellTag, std::vector<TTableId>>* externalCellTagToTableIds,
    const NTabletClient::NProto::TExternalCellTagToTableIds& protoExternalCellTagToTableIds)
{
    externalCellTagToTableIds->first = TCellTag(protoExternalCellTagToTableIds.external_cell_tag());
    FromProto(&externalCellTagToTableIds->second, protoExternalCellTagToTableIds.table_ids());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
