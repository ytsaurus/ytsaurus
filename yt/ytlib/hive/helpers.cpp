#include "helpers.h"
#include "hive_service_proxy.h"
#include "cell_directory.h"
#include "private.h"

namespace NYT::NHiveClient {

////////////////////////////////////////////////////////////////////////////////

const auto& Logger = HiveClientLogger;

////////////////////////////////////////////////////////////////////////////////

TFuture<void> SyncHiveCellWithOthers(
    const TCellDirectoryPtr& cellDirectory,
    const std::vector<TCellId>& srcCellIds,
    TCellId dstCellId,
    TDuration rpcTimeout)
{
    YT_LOG_DEBUG("Started synchronizing Hive cell with others (SrcCellIds: %v, DstCellId: %v)",
        srcCellIds,
        dstCellId);

    auto channel = cellDirectory->GetChannelOrThrow(dstCellId);
    THiveServiceProxy proxy(std::move(channel));

    auto req = proxy.SyncWithOthers();
    req->SetTimeout(rpcTimeout);
    ToProto(req->mutable_src_cell_ids(), srcCellIds);

    return req->Invoke()
        .Apply(BIND([=] (const THiveServiceProxy::TErrorOrRspSyncWithOthersPtr& rspOrError) {
            THROW_ERROR_EXCEPTION_IF_FAILED(
                rspOrError,
                "Error synchronizing Hive cell %v with %v",
                dstCellId,
                srcCellIds);
            YT_LOG_DEBUG("Finished synchronizing Hive cell with others (SrcCellIds: %v, DstCellId: %v)",
                srcCellIds,
                dstCellId);
        }));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHiveClient
