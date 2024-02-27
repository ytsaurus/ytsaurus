#include "client_impl.h"

#include <yt/yt/ytlib/api/native/tablet_helpers.h>
#include <yt/yt/ytlib/api/native/transaction.h>

#include <yt/yt/ytlib/chunk_client/chunk_fragment_reader.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader_options.h>

#include <yt/yt/ytlib/hive/cell_directory.h>
#include <yt/yt/ytlib/hive/cell_directory_synchronizer.h>

#include <yt/yt/ytlib/lease_client/lease_service_proxy.h>

#include <yt/yt/ytlib/node_tracker_client/node_status_directory.h>

#include <yt/yt/ytlib/table_client/hunks.h>

#include <yt/yt/ytlib/tablet_client/tablet_service_proxy.h>

#include <yt/yt/ytlib/transaction_client/action.h>

#include <yt/yt/client/tablet_client/table_mount_cache.h>

#include <library/cpp/iterator/enumerate.h>
#include <library/cpp/iterator/zip.h>

namespace NYT::NApi::NNative {

using namespace NConcurrency;
using namespace NChunkClient;
using namespace NLeaseClient;
using namespace NHydra;
using namespace NNodeTrackerClient;
using namespace NObjectClient;
using namespace NQueryClient;
using namespace NTableClient;
using namespace NTabletClient;
using namespace NTransactionClient;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

std::vector<TSharedRef> TClient::DoReadHunks(
    const std::vector<THunkDescriptor>& requests,
    const TReadHunksOptions& options)
{
    auto reader = CreateChunkFragmentReader(
        options.Config,
        this,
        CreateTrivialNodeStatusDirectory(),
        /*profiler*/ {},
        /*throttlerProvider*/ {});

    std::vector<IChunkFragmentReader::TChunkFragmentRequest> readerRequests;
    readerRequests.reserve(requests.size());
    for (const auto& request : requests) {
        readerRequests.push_back(IChunkFragmentReader::TChunkFragmentRequest{
            .ChunkId = request.ChunkId,
            .ErasureCodec = request.ErasureCodec,
            .Length = request.Length,
            .BlockIndex = request.BlockIndex,
            .BlockOffset = request.BlockOffset,
            .BlockSize = request.BlockSize
        });
    }

    auto response = WaitFor(reader->ReadFragments(/*options*/ {}, readerRequests))
        .ValueOrThrow();
    const auto& fragments = response.Fragments;
    YT_VERIFY(fragments.size() == requests.size());
    if (!options.ParseHeader) {
        return fragments;
    }

    std::vector<TSharedRef> payloads;
    payloads.reserve(fragments.size());
    for (int requestIndex = 0; requestIndex < std::ssize(requests); ++requestIndex) {
        const auto& request = readerRequests[requestIndex];
        const auto& fragment = fragments[requestIndex];
        payloads.push_back(GetAndValidateHunkPayload(fragment, request));
    }

    return payloads;
}

std::vector<THunkDescriptor> TClient::DoWriteHunks(
    const TYPath& path,
    int tabletIndex,
    const std::vector<TSharedRef>& payloads,
    const TWriteHunksOptions& options)
{
    auto tableMountInfo = WaitFor(GetTableMountCache()->GetTableInfo(path))
        .ValueOrThrow();
    auto tabletInfo = tableMountInfo->GetTabletByIndexOrThrow(tabletIndex);
    auto cellChannel = GetCellChannelOrThrow(tabletInfo->CellId);

    TTabletServiceProxy proxy(cellChannel);

    auto req = proxy.WriteHunks();
    req->SetTimeout(options.Timeout);
    ToProto(req->mutable_tablet_id(), tabletInfo->TabletId);
    req->set_mount_revision(tabletInfo->MountRevision);

    for (const auto& payload : payloads) {
        req->Attachments().push_back(payload);
    }

    auto rsp = WaitFor(req->Invoke())
        .ValueOrThrow();

    std::vector<THunkDescriptor> descriptors;
    descriptors.reserve(rsp->descriptors_size());
    for (const auto& protoDescriptor : rsp->descriptors()) {
        descriptors.push_back(THunkDescriptor{
            .ChunkId = FromProto<TChunkId>(protoDescriptor.chunk_id()),
            .ErasureCodec = FromProto<NErasure::ECodec>(protoDescriptor.erasure_codec()),
            .BlockIndex = protoDescriptor.record_index(),
            .BlockOffset = protoDescriptor.record_offset(),
            .Length = protoDescriptor.length(),
            .BlockSize = protoDescriptor.record_size(),
        });
    }

    return descriptors;
}

void TClient::DoLockHunkStore(
    const TYPath& path,
    int tabletIndex,
    TStoreId storeId,
    TTabletId tabletId,
    const TLockHunkStoreOptions& options)
{
    DoToggleHunkStoreLock(
        path,
        tabletIndex,
        storeId,
        tabletId,
        /*lock*/ true,
        options);
}

void TClient::DoUnlockHunkStore(
    const TYPath& path,
    int tabletIndex,
    TStoreId storeId,
    TTabletId tabletId,
    const TUnlockHunkStoreOptions& options)
{
    DoToggleHunkStoreLock(
        path,
        tabletIndex,
        storeId,
        tabletId,
        /*lock*/ false,
        options);
}

void TClient::DoIssueLease(
    TCellId cellId,
    TObjectId leaseId,
    const TIssueLeaseOptions& options)
{
    const auto& cellDirectorySynchronizer = Connection_->GetCellDirectorySynchronizer();
    WaitFor(cellDirectorySynchronizer->Sync())
        .ThrowOnError();

    auto channel = GetCellChannelOrThrow(cellId);
    TLeaseServiceProxy proxy(std::move(channel));
    auto req = proxy.IssueLease();
    req->SetTimeout(options.Timeout);
    ToProto(req->mutable_lease_id(), leaseId);
    WaitFor(req->Invoke())
        .ThrowOnError();
}

void TClient::DoRevokeLease(
    TCellId cellId,
    TObjectId leaseId,
    bool force,
    const TRevokeLeaseOptions& options)
{
    const auto& cellDirectorySynchronizer = Connection_->GetCellDirectorySynchronizer();
    WaitFor(cellDirectorySynchronizer->Sync())
        .ThrowOnError();

    auto channel = GetCellChannelOrThrow(cellId);
    TLeaseServiceProxy proxy(std::move(channel));
    auto req = proxy.RevokeLease();
    req->SetTimeout(options.Timeout);
    ToProto(req->mutable_lease_id(), leaseId);
    req->set_force(force);
    WaitFor(req->Invoke())
        .ThrowOnError();
}

void TClient::DoReferenceLease(
    TCellId cellId,
    TObjectId leaseId,
    bool persistent,
    bool force,
    const TReferenceLeaseOptions& options)
{
    const auto& cellDirectorySynchronizer = Connection_->GetCellDirectorySynchronizer();
    WaitFor(cellDirectorySynchronizer->Sync())
        .ThrowOnError();

    auto channel = GetCellChannelOrThrow(cellId);
    TLeaseServiceProxy proxy(std::move(channel));
    auto req = proxy.ReferenceLease();
    req->SetTimeout(options.Timeout);
    ToProto(req->mutable_lease_id(), leaseId);
    req->set_persistent(persistent);
    req->set_force(force);
    WaitFor(req->Invoke())
        .ThrowOnError();
}

void TClient::DoUnreferenceLease(
    TCellId cellId,
    TObjectId leaseId,
    bool persistent,
    const TUnreferenceLeaseOptions& options)
{
    const auto& cellDirectorySynchronizer = Connection_->GetCellDirectorySynchronizer();
    WaitFor(cellDirectorySynchronizer->Sync())
        .ThrowOnError();

    auto channel = GetCellChannelOrThrow(cellId);
    TLeaseServiceProxy proxy(std::move(channel));
    auto req = proxy.UnreferenceLease();
    req->SetTimeout(options.Timeout);
    ToProto(req->mutable_lease_id(), leaseId);
    req->set_persistent(persistent);
    WaitFor(req->Invoke())
        .ThrowOnError();
}

void TClient::DoToggleHunkStoreLock(
    const TYPath& path,
    int tabletIndex,
    TStoreId storeId,
    TTabletId lockerTabletId,
    bool lock,
    const TTimeoutOptions& /*options*/)
{
    auto tableMountInfo = WaitFor(GetTableMountCache()->GetTableInfo(path))
        .ValueOrThrow();
    auto tabletInfo = tableMountInfo->GetTabletByIndexOrThrow(tabletIndex);

    auto transaction = WaitFor(StartNativeTransaction(
        NTransactionClient::ETransactionType::Tablet,
        /*options*/ {}))
        .ValueOrThrow();

    NTabletClient::NProto::TReqToggleHunkTabletStoreLock request;
    ToProto(request.mutable_tablet_id(), tabletInfo->TabletId);
    request.set_mount_revision(tabletInfo->MountRevision);
    ToProto(request.mutable_store_id(), storeId);
    ToProto(request.mutable_locker_tablet_id(), lockerTabletId);
    request.set_lock(lock);

    auto actionData = NTransactionClient::MakeTransactionActionData(request);
    transaction->AddAction(tabletInfo->CellId, actionData);

    TTransactionCommitOptions commitOptions{
        .Force2PC = true
    };
    WaitFor(transaction->Commit(commitOptions))
        .ThrowOnError();
}

std::vector<TErrorOr<i64>> TClient::DoGetOrderedTabletSafeTrimRowCount(
    const std::vector<TGetOrderedTabletSafeTrimRowCountRequest>& requests,
    const TGetOrderedTabletSafeTrimRowCountOptions& options)
{
    const auto& tableMountCache = GetTableMountCache();
    const auto& cellDirectory = Connection_->GetCellDirectory();

    using TSubrequest = NQueryClient::NProto::TReqGetOrderedTabletSafeTrimRowCount::TSubrequest;
    THashMap<TString, std::vector<std::pair<TSubrequest, int>>> addressToSubrequests;

    std::vector<TFuture<TTableMountInfoPtr>> asyncTableInfos;
    asyncTableInfos.reserve(requests.size());

    for (const auto& request : requests) {
        asyncTableInfos.push_back(tableMountCache->GetTableInfo(request.Path));
    }

    auto tableInfos = WaitForFast(AllSet(asyncTableInfos))
        .ValueOrThrow();

    std::vector<TErrorOr<i64>> results;
    results.resize(requests.size());

    for (const auto& [requestIndex, request] : Enumerate(requests)) {
        if (!tableInfos[requestIndex].IsOK()) {
            results[requestIndex] = TError(tableInfos[requestIndex]);
            continue;
        }

        auto tableInfo = tableInfos[requestIndex].Value();
        auto tabletInfo = tableInfo->GetTabletByIndexOrThrow(request.TabletIndex);

        TSubrequest subrequest;
        ToProto(subrequest.mutable_tablet_id(), tabletInfo->TabletId);
        ToProto(subrequest.mutable_cell_id(), tabletInfo->CellId);
        subrequest.set_mount_revision(tabletInfo->MountRevision);
        subrequest.set_timestamp(request.Timestamp);

        auto cellDescriptor = cellDirectory->GetDescriptorByCellIdOrThrow(tabletInfo->CellId);
        const auto& primaryPeerDescriptor = GetPrimaryTabletPeerDescriptor(*cellDescriptor, NHydra::EPeerKind::Leader);
        auto cellAddress = primaryPeerDescriptor.GetAddressOrThrow(Connection_->GetNetworks());

        addressToSubrequests[cellAddress].emplace_back(std::move(subrequest), requestIndex);
    }

    std::vector<TFuture<TQueryServiceProxy::TRspGetOrderedTabletSafeTrimRowCountPtr>> asyncNodeResponses;

    for (const auto& [address, subrequests] : addressToSubrequests) {
        auto channel = Connection_->GetChannelFactory()->CreateChannel(address);
        TQueryServiceProxy proxy(channel);
        auto req = proxy.GetOrderedTabletSafeTrimRowCount();
        req->SetTimeout(options.Timeout);
        for (const auto& [subrequest, index] : subrequests) {
            *req->add_subrequests() = subrequest;
        }
        asyncNodeResponses.push_back(req->Invoke());
    }

    auto nodeResponses = WaitFor(AllSet(asyncNodeResponses))
        .ValueOrThrow();
    YT_VERIFY(nodeResponses.size() == addressToSubrequests.size());

    for (const auto& [nodeRequest, nodeResponseOrError] : Zip(addressToSubrequests, nodeResponses)) {
        const auto& subrequests = nodeRequest.second;
        if (!nodeResponseOrError.IsOK()) {
            for (const auto& [subrequest, requestIndex] : subrequests) {
                results[requestIndex] = TError(nodeResponseOrError);
            }

            continue;
        }

        const auto& nodeResponse = nodeResponseOrError.Value();

        if (std::ssize(subrequests) != nodeResponse->subresponses_size()) {
            THROW_ERROR_EXCEPTION(
                "Invalid number of subresponses: expected %v, actual %v",
                subrequests.size(),
                nodeResponse->subresponses_size());
        }

        for (const auto& [subresponseIndex, subresponse] : Enumerate(nodeResponse->subresponses())) {
            auto requestIndex = subrequests[subresponseIndex].second;
            if (subresponse.has_error()) {
                results[requestIndex] = FromProto<TError>(subresponse.error());
            } else {
                results[requestIndex] = subresponse.safe_trim_row_count();
            }
        }
    }

    return results;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative
