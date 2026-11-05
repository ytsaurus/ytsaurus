#include "push_based_shuffle_registry.h"

#include "private.h"

#include <yt/yt/ytlib/distributed_chunk_session_client/session_pool.h>

#include <yt/yt/core/rpc/public.h>

namespace NYT::NControllerAgent {

using namespace NDistributedChunkSessionClient;

////////////////////////////////////////////////////////////////////////////////

constinit const auto Logger = ControllerAgentLogger;

////////////////////////////////////////////////////////////////////////////////

void TPushBasedShuffleRegistry::OnSchedulerConnected(TIncarnationId incarnationId)
{
    YT_ASSERT_THREAD_AFFINITY(ControlThread);

    int droppedShuffleCount = ResetIncarnation(incarnationId);

    YT_TLOG_INFO("Set new push-based shuffle incarnation")
        .With("IncarnationId", incarnationId)
        .With("DroppedShuffleCount", droppedShuffleCount);
}

void TPushBasedShuffleRegistry::Cleanup()
{
    YT_ASSERT_THREAD_AFFINITY(ControlThread);

    int droppedShuffleCount = ResetIncarnation(/*incarnationId*/ {});

    YT_TLOG_INFO("Cleaned up push-based shuffle state")
        .With("DroppedShuffleCount", droppedShuffleCount);
}

int TPushBasedShuffleRegistry::ResetIncarnation(TIncarnationId incarnationId)
{
    decltype(IdToPool_) stalePools;

    {
        auto guard = WriterGuard(Lock_);

        stalePools.swap(IdToPool_);
        IncarnationId_ = incarnationId;
    }

    return std::ssize(stalePools);
}

void TPushBasedShuffleRegistry::RegisterShuffle(
    TIncarnationId incarnationId,
    TOperationId operationId,
    TWeakPtr<IDistributedChunkSessionPool> pool)
{
    {
        auto guard = WriterGuard(Lock_);

        ValidateIncarnation(incarnationId);

        if (!IdToPool_.emplace(operationId, std::move(pool)).second) {
            THROW_ERROR_EXCEPTION(
                "Operation %v already has a registered push-based shuffle",
                operationId);
        }
    }

    YT_TLOG_DEBUG("Push-based shuffle registered")
        .With("OperationId", operationId);
}

void TPushBasedShuffleRegistry::UnregisterShuffle(TIncarnationId incarnationId, TOperationId operationId)
{
    bool unregistered = false;

    {
        auto guard = WriterGuard(Lock_);

        if (IncarnationId_ != incarnationId) {
            return;
        }

        unregistered = IdToPool_.erase(operationId) > 0;
    }

    YT_TLOG_DEBUG_IF(unregistered, "Push-based shuffle unregistered")
        .With("OperationId", operationId);
}

IDistributedChunkSessionPoolPtr TPushBasedShuffleRegistry::GetShufflePoolOrThrow(
    TIncarnationId incarnationId,
    TOperationId operationId) const
{
    auto guard = ReaderGuard(Lock_);

    ValidateIncarnation(incarnationId);

    auto it = IdToPool_.find(operationId);
    if (it == IdToPool_.end()) {
        THROW_ERROR_EXCEPTION(
            NRpc::EErrorCode::TransientFailure,
            "Operation %v has no registered push-based shuffle",
            operationId);
    }

    auto pool = it->second.Lock();
    if (!pool) {
        THROW_ERROR_EXCEPTION(
            NRpc::EErrorCode::TransientFailure,
            "Operation %v no longer has a shuffle session pool",
            operationId);
    }

    return pool;
}

void TPushBasedShuffleRegistry::ValidateIncarnation(TIncarnationId incarnationId) const
{
    YT_ASSERT_SPINLOCK_AFFINITY(Lock_);

    THROW_ERROR_EXCEPTION_IF(
        !IncarnationId_,
        EErrorCode::AgentDisconnected,
        "Controller agent disconnected");

    if (IncarnationId_ != incarnationId) {
        THROW_ERROR_EXCEPTION(
            EErrorCode::IncarnationMismatch,
            "Controller agent incarnation mismatch: expected %v, got %v",
            IncarnationId_,
            incarnationId);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent
