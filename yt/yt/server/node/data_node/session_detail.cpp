#include "session_detail.h"

#include "bootstrap.h"
#include "private.h"
#include "config.h"
#include "location.h"
#include "session_manager.h"

#include <yt/yt/core/profiling/timing.h>

namespace NYT::NDataNode {

////////////////////////////////////////////////////////////////////////////////

TProbePutBlocksRequestSupplier::TProbePutBlocksRequestSupplier(TSessionId sessionId)
    : SessionId_(sessionId)
{ }

TSessionId TProbePutBlocksRequestSupplier::GetSessionId() const
{
    return SessionId_;
}

void TProbePutBlocksRequestSupplier::CancelRequests()
{
    auto guard = Guard(Lock_);
    Canceled_ = true;
}

bool TProbePutBlocksRequestSupplier::IsCanceled() const
{
    auto guard = Guard(Lock_);
    return Canceled_;
}

i64 TProbePutBlocksRequestSupplier::GetCurrentApprovedMemory() const
{
    auto guard = Guard(Lock_);
    return ApprovedMemory_;
}

i64 TProbePutBlocksRequestSupplier::GetMaxRequestedMemory() const
{
    auto guard = Guard(Lock_);
    return MaxRequestedMemory_;
}

std::optional<TProbePutBlocksRequestSupplier::TRequest> TProbePutBlocksRequestSupplier::DequeueMinRequest()
{
    auto guard = Guard(Lock_);
    if (Requests_.empty()) {
        return std::nullopt;
    }

    auto request = *Requests_.begin();
    Requests_.erase(Requests_.begin());

    return request;
}

void TProbePutBlocksRequestSupplier::ApproveRequest(TLocationMemoryGuard&& memoryGuard, TRequest request)
{
    auto guard = Guard(Lock_);

    YT_VERIFY(request.CumulativeBlockSize > ApprovedMemory_);
    YT_VERIFY(memoryGuard.GetUseLegacyUsedMemory() == false);
    YT_VERIFY(memoryGuard.GetSize() == request.CumulativeBlockSize - ApprovedMemory_);
    YT_VERIFY(!MemoryGuard_ || MemoryGuard_.GetUseLegacyUsedMemory() == false);

    if (MemoryGuard_) {
        YT_VERIFY(memoryGuard.GetOwner() == MemoryGuard_.GetOwner());

        auto acquiredMemory = memoryGuard.GetSize();
        memoryGuard.Release();
        MemoryGuard_.IncreaseSize(acquiredMemory);
    } else {
        MemoryGuard_ = std::move(memoryGuard);
    }

    ApprovedMemory_ = request.CumulativeBlockSize;

    while (!Requests_.empty() && Requests_.begin()->CumulativeBlockSize <= ApprovedMemory_) {
        Requests_.erase(Requests_.begin());
    }
}

void TProbePutBlocksRequestSupplier::PushRequest(TRequest request)
{
    auto guard = Guard(Lock_);

    if (request.CumulativeBlockSize <= ApprovedMemory_) {
        return;
    }

    Requests_.insert(std::move(request));
    MaxRequestedMemory_ = std::max(MaxRequestedMemory_, request.CumulativeBlockSize);
}

void TProbePutBlocksRequestSupplier::ReleaseResourcesForPutBlocks(i64 memory)
{
    auto guard = Guard(Lock_);
    MemoryGuard_.DecreaseSize(memory);
}

////////////////////////////////////////////////////////////////////////////////

using namespace NRpc;
using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NNodeTrackerClient;
using namespace NClusterNode;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

TSessionBase::TSessionBase(
    TDataNodeConfigPtr config,
    IBootstrap* bootstrap,
    TSessionId sessionId,
    const TSessionOptions& options,
    TStoreLocationPtr location,
    TLease lease,
    TLockedChunkGuard lockedChunkGuard,
    IChunkWriter::TWriteBlocksOptions writeBlocksOptions)
    : Config_(std::move(config))
    , Bootstrap_(bootstrap)
    , SessionId_(sessionId)
    , Options_(options)
    , Location_(location)
    , Lease_(std::move(lease))
    , MasterEpoch_(Bootstrap_->GetMasterEpoch())
    , SessionInvoker_(CreateSerializedInvoker(Location_->GetAuxPoolInvoker()))
    , Logger(DataNodeLogger().WithTag("LocationId: %v, ChunkId: %v",
        Location_->GetId(),
        SessionId_))
    , StartTime_(TInstant::Now())
    , LockedChunkGuard_(std::move(lockedChunkGuard))
    , WriteBlocksOptions_(std::move(writeBlocksOptions))
    , ProbePutBlocksRequestSupplier_(New<TProbePutBlocksRequestSupplier>(SessionId_))
    , UseProbePutBlocks_(options.UseProbePutBlocks)
{
    YT_VERIFY(Bootstrap_);
    YT_VERIFY(Location_);
    YT_VERIFY(Lease_);
    YT_VERIFY(LockedChunkGuard_);
}

TChunkId TSessionBase::GetChunkId() const&
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    return SessionId_.ChunkId;
}

TSessionId TSessionBase::GetId() const&
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    return SessionId_;
}

ESessionType TSessionBase::GetType() const
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    switch (Options_.WorkloadDescriptor.Category) {
        case EWorkloadCategory::SystemRepair:
            return ESessionType::Repair;
        case EWorkloadCategory::SystemReplication:
            return ESessionType::Replication;
        default:
            return ESessionType::User;
    }
}

TInstant TSessionBase::GetStartTime() const
{
    return StartTime_;
}

TMasterEpoch TSessionBase::GetMasterEpoch() const
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    return MasterEpoch_;
}

const TWorkloadDescriptor& TSessionBase::GetWorkloadDescriptor() const
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    return Options_.WorkloadDescriptor;
}

const TSessionOptions& TSessionBase::GetSessionOptions() const
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    return Options_;
}

const TStoreLocationPtr& TSessionBase::GetStoreLocation() const
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    return Location_;
}

TFuture<void> TSessionBase::Start()
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    YT_LOG_DEBUG("Starting session");

    return
        BIND(&TSessionBase::DoStart, MakeStrong(this))
            .AsyncVia(SessionInvoker_)
            .Run()
            .Apply(BIND([=, this, this_ = MakeStrong(this)] (const TError& error) {
                YT_ASSERT_INVOKER_AFFINITY(SessionInvoker_);

                YT_VERIFY(!Active_);
                Active_ = true;

                if (!error.IsOK()) {
                    YT_LOG_DEBUG(error, "Session has failed to start");
                    Cancel(error);
                    THROW_ERROR(error);
                }

                YT_LOG_DEBUG("Session started");

                if (!PendingCancelationError_.IsOK()) {
                    Cancel(PendingCancelationError_);
                }
            }).AsyncVia(SessionInvoker_))
            // TODO(babenko): session start cancelation is not properly supported
            .ToUncancelable();
}

void TSessionBase::Ping()
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    // Let's be generous and accept pings in any state.
    TLeaseManager::RenewLease(Lease_);
}

void TSessionBase::Cancel(const TError& error)
{
    YT_ASSERT_THREAD_AFFINITY_ANY();
    YT_VERIFY(!error.IsOK());

    SessionInvoker_->Invoke(
        BIND([=, this, this_ = MakeStrong(this)] {
            YT_ASSERT_INVOKER_AFFINITY(SessionInvoker_);

            if (Canceled_.load()) {
                return;
            }

            if (!Active_) {
                YT_LOG_DEBUG(error, "Session will be canceled after becoming active");
                PendingCancelationError_ = error;
                return;
            }

            YT_LOG_DEBUG(error, "Canceling session");

            TLeaseManager::CloseLease(Lease_);
            Active_ = false;
            Canceled_.store(true);
            ProbePutBlocksRequestSupplier_->CancelRequests();
            Location_->CheckProbePutBlocksRequests();

            DoCancel(error);
        }));
}

void TSessionBase::OnUnregistered()
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    UnregisteredEvent_.Set();
}

void TSessionBase::UnlockChunk()
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    auto guard = std::move(LockedChunkGuard_);
}

TFuture<void> TSessionBase::GetUnregisteredEvent()
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    return UnregisteredEvent_.ToFuture();
}

TFuture<ISession::TFinishResult> TSessionBase::Finish(
    const TRefCountedChunkMetaPtr& chunkMeta,
    std::optional<int> blockCount,
    bool truncateExtraBlocks)
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    return
        BIND([=, this, this_ = MakeStrong(this)] {
            YT_ASSERT_INVOKER_AFFINITY(SessionInvoker_);

            ValidateActive();

            YT_LOG_DEBUG("Finishing session");

            TLeaseManager::CloseLease(Lease_);
            Active_ = false;

            return DoFinish(chunkMeta, blockCount, truncateExtraBlocks);
        })
        .AsyncVia(SessionInvoker_)
        .Run();
}

TLocationMemoryGuard TSessionBase::GetMemoryForPutBlocks(i64 memory)
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    ProbePutBlocksRequestSupplier_->ReleaseResourcesForPutBlocks(memory);

    return Location_->AcquireLocationMemory(true, {}, EIODirection::Write, GetWorkloadDescriptor(), memory);
}

i64 TSessionBase::GetApprovedCumulativeBlockSize() const
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    return ProbePutBlocksRequestSupplier_->GetCurrentApprovedMemory();
}

i64 TSessionBase::GetMaxRequestedCumulativeBlockSize() const
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    return ProbePutBlocksRequestSupplier_->GetMaxRequestedMemory();
}

bool TSessionBase::ShouldUseProbePutBlocks() const
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    return UseProbePutBlocks_;
}

void TSessionBase::ProbePutBlocks(i64 requestedCumulativeMemorySize)
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    YT_LOG_INFO("ProbePutBlocks request pushed "
        "(SessionId: %v, RequestedCumulativeBlockSize: %v)",
        SessionId_, requestedCumulativeMemorySize);

    ProbePutBlocksRequestSupplier_->PushRequest({
        .CumulativeBlockSize = requestedCumulativeMemorySize,
        .WorkloadDescriptor = GetWorkloadDescriptor(),
    });

    Location_->PushProbePutBlocksRequestSupplier(ProbePutBlocksRequestSupplier_);
}

TFuture<NIO::TIOCounters> TSessionBase::PutBlocks(
    int startBlockIndex,
    std::vector<TBlock> blocks,
    i64 cumulativeBlockSize,
    bool enableCaching)
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    return BIND(
        [
            =,
            this,
            this_ = MakeStrong(this),
            blocks = std::move(blocks)
        ] () mutable {
            YT_ASSERT_INVOKER_AFFINITY(SessionInvoker_);

            ValidateActive();
            Ping();

            return DoPutBlocks(startBlockIndex, std::move(blocks), cumulativeBlockSize, enableCaching);
        })
        .AsyncVia(SessionInvoker_)
        .Run();
}

TFuture<TDataNodeServiceProxy::TRspPutBlocksPtr> TSessionBase::SendBlocks(
    int startBlockIndex,
    int blockCount,
    i64 cumulativeBlockSize,
    const TNodeDescriptor& targetDescriptor)
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    return
        BIND([=, this, this_ = MakeStrong(this)] {
            YT_ASSERT_INVOKER_AFFINITY(SessionInvoker_);

            ValidateActive();
            Ping();

            return DoSendBlocks(startBlockIndex, blockCount, cumulativeBlockSize, targetDescriptor);
        })
        .AsyncVia(SessionInvoker_)
        .Run();
}

TFuture<ISession::TFlushBlocksResult> TSessionBase::FlushBlocks(int blockIndex)
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    return
        BIND([=, this, this_ = MakeStrong(this)] {
            YT_ASSERT_INVOKER_AFFINITY(SessionInvoker_);

            ValidateActive();
            Ping();

            return DoFlushBlocks(blockIndex);
        })
        .AsyncVia(SessionInvoker_)
        .Run();
}

void TSessionBase::ValidateActive() const
{
    YT_ASSERT_INVOKER_AFFINITY(SessionInvoker_);

    if (!Active_) {
        THROW_ERROR_EXCEPTION("Session is not active");
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNode
