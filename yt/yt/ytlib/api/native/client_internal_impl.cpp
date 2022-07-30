#include "client_impl.h"

#include <yt/yt/ytlib/api/native/transaction.h>

#include <yt/yt/ytlib/chunk_client/chunk_fragment_reader.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader_options.h>

#include <yt/yt/ytlib/node_tracker_client/node_status_directory.h>

#include <yt/yt/ytlib/tablet_client/tablet_service_proxy.h>

#include <yt/yt/ytlib/transaction_client/action.h>

#include <yt/yt/client/tablet_client/table_mount_cache.h>

namespace NYT::NApi::NNative {

using namespace NConcurrency;
using namespace NChunkClient;
using namespace NNodeTrackerClient;
using namespace NObjectClient;
using namespace NTabletClient;
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
        /*profiler*/ {});

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

    return response.Fragments;
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
            .BlockIndex = protoDescriptor.record_index(),
            .BlockOffset = protoDescriptor.record_offset(),
            .Length = protoDescriptor.length(),
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

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative
