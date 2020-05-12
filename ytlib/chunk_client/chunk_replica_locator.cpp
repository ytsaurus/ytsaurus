#include "chunk_replica_locator.h"
#include "dispatcher.h"

#include <yt/ytlib/api/native/client.h>

#include <yt/client/object_client/helpers.h>

#include <yt/core/concurrency/delayed_executor.h>
#include <yt/core/concurrency/thread_affinity.h>

namespace NYT::NChunkClient {

using namespace NNodeTrackerClient;
using namespace NObjectClient;
using namespace NApi;
using namespace NConcurrency;

using NYT::FromProto;

////////////////////////////////////////////////////////////////////////////////

TChunkReplicaLocator::TChunkReplicaLocator(
    NNative::IClientPtr client,
    TNodeDirectoryPtr nodeDirectory,
    TChunkId chunkId,
    TDuration expirationTime,
    TChunkReplicaList initialReplicas,
    NLogging::TLogger logger)
    : NodeDirectory_(std::move(nodeDirectory))
    , ChunkId_(chunkId)
    , ExpirationTime_(expirationTime)
    , Channel_(client->GetMasterChannelOrThrow(
        EMasterChannelKind::Follower,
        CellTagFromId(ChunkId_)))
    , Logger(std::move(logger))
{
    if (!initialReplicas.empty()) {
        ReplicasPromise_ = MakePromise(initialReplicas);
    }
}

TFuture<TChunkReplicaList> TChunkReplicaLocator::GetReplicas()
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto guard = Guard(Lock_);

    if (!ReplicasPromise_) {
        ReplicasPromise_ = NewPromise<TChunkReplicaList>();
        
        auto locateChunkCallback = BIND(&TChunkReplicaLocator::LocateChunk, MakeStrong(this))
            .Via(TDispatcher::Get()->GetReaderInvoker());

        // Don't ask master for replicas too often.
        if (Timestamp_ + ExpirationTime_ > TInstant::Now()) {
            auto deadline = Timestamp_ + ExpirationTime_;
            YT_LOG_DEBUG("Will fetch chunk replicas in a while (Deadline: %v)",
                deadline);
            TDelayedExecutor::Submit(std::move(locateChunkCallback), deadline);
        } else {
            locateChunkCallback.Run();
        }
    }

    return ReplicasPromise_;
}

void TChunkReplicaLocator::LocateChunk()
{
    VERIFY_THREAD_AFFINITY_ANY();

    YT_LOG_DEBUG("Requesting chunk replicas from master");

    TChunkServiceProxy proxy(Channel_);
    auto req = proxy.LocateChunks();
    ToProto(req->add_subrequests(), ChunkId_);
    req->Invoke().Subscribe(
        BIND(&TChunkReplicaLocator::OnChunkLocated, MakeStrong(this)));
}

void TChunkReplicaLocator::OnChunkLocated(const TChunkServiceProxy::TErrorOrRspLocateChunksPtr& rspOrError)
{
    VERIFY_THREAD_AFFINITY_ANY();

    TPromise<TChunkReplicaList> promise;
    {
        TGuard<TSpinLock> guard(Lock_);
        Timestamp_ = TInstant::Now();
        promise = ReplicasPromise_;
    }

    if (!rspOrError.IsOK()) {
        promise.Set(TError(rspOrError));
        return;
    }

    const auto& rsp = rspOrError.Value();
    YT_VERIFY(rsp->subresponses_size() == 1);
    const auto& subresponse = rsp->subresponses(0);
    if (subresponse.missing()) {
        promise.Set(TError(
            NChunkClient::EErrorCode::NoSuchChunk,
            "No such chunk %v",
            ChunkId_));
        return;
    }

    NodeDirectory_->MergeFrom(rsp->node_directory());
    auto replicas = FromProto<TChunkReplicaList>(subresponse.replicas());

    YT_LOG_DEBUG("Chunk located (Replicas: %v)",
        MakeFormattableView(replicas, TChunkReplicaAddressFormatter(NodeDirectory_)));

    ReplicasLocated_.Fire(replicas);
    promise.Set(replicas);
}

void TChunkReplicaLocator::DiscardReplicas(const TFuture<TChunkReplicaList>& future)
{
    YT_VERIFY(future.IsSet());
    const auto& replicasOrError = future.Get();

    auto guard = Guard(Lock_);
    if (ReplicasPromise_.ToFuture() == future) {
        ReplicasPromise_.Reset();
        if (replicasOrError.IsOK()) {
            const auto& replicas = replicasOrError.Value();
            YT_LOG_DEBUG("Chunk replicas discarded (Replicas: %v)",
                MakeFormattableView(replicas, TChunkReplicaAddressFormatter(NodeDirectory_)));
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
