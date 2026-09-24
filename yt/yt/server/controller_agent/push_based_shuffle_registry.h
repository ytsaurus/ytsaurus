#pragma once

#include "private.h"

#include <yt/yt/ytlib/distributed_chunk_session_client/public.h>

#include <yt/yt/core/actions/public.h>

#include <yt/yt/core/concurrency/public.h>
#include <yt/yt/core/concurrency/thread_affinity.h>

#include <library/cpp/yt/threading/rw_spin_lock.h>

namespace NYT::NControllerAgent {

////////////////////////////////////////////////////////////////////////////////

//! Routes a shuffle write session request to the session pool of its operation, rejecting
//! a stale incarnation. Pools are built and owned by operation controllers.
class TPushBasedShuffleRegistry
    : public TRefCounted
{
public:
    explicit TPushBasedShuffleRegistry(const TControllerAgentConfigPtr& config);

    //! \note Thread affinity: any
    const IInvokerPtr& GetInvoker() const;

    //! \note Thread affinity: any
    void UpdateConfig(const TControllerAgentConfigPtr& config);

    //! \note Thread affinity: ControlThread
    void OnSchedulerConnected(TIncarnationId incarnationId);

    //! \note Thread affinity: ControlThread
    void Cleanup();

    //! Rejects a registration stamped with anything but the current incarnation.
    /*!
     *  \note Thread affinity: any
     */
    void RegisterShuffle(
        TIncarnationId incarnationId,
        TOperationId operationId,
        TWeakPtr<NDistributedChunkSessionClient::IDistributedChunkSessionPool> pool);

    //! Ignores a stale incarnation: the entry may already belong to the live one.
    /*!
     *  \note Thread affinity: any
     */
    void UnregisterShuffle(TIncarnationId incarnationId, TOperationId operationId);

    //! \note Thread affinity: any
    NDistributedChunkSessionClient::IDistributedChunkSessionPoolPtr GetShufflePoolOrThrow(
        TIncarnationId incarnationId,
        TOperationId operationId) const;

private:
    const NConcurrency::IThreadPoolPtr ThreadPool_;
    const IInvokerPtr Invoker_;

    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, Lock_);
    TIncarnationId IncarnationId_;
    THashMap<TOperationId, TWeakPtr<NDistributedChunkSessionClient::IDistributedChunkSessionPool>> IdToPool_;

    void ValidateIncarnation(TIncarnationId incarnationId) const;

    int ResetIncarnation(TIncarnationId incarnationId);

    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);
};

DEFINE_REFCOUNTED_TYPE(TPushBasedShuffleRegistry)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent
