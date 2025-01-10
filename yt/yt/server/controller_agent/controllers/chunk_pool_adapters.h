#pragma once

#include "private.h"

#include "task_host.h"

#include <yt/yt/server/lib/chunk_pools/chunk_pool.h>

namespace NYT::NControllerAgent::NControllers {

////////////////////////////////////////////////////////////////////////////////

class TChunkPoolInputAdapterBase
    : public virtual NChunkPools::IPersistentChunkPoolInput
{
public:
    TChunkPoolInputAdapterBase() = default;

    explicit TChunkPoolInputAdapterBase(NChunkPools::IPersistentChunkPoolInputPtr underlyingInput);

    TCookie AddWithKey(NChunkPools::TChunkStripePtr stripe, NChunkPools::TChunkStripeKey key) override;

    TCookie Add(NChunkPools::TChunkStripePtr stripe) override;

    void Suspend(TCookie cookie) override;

    void Resume(TCookie cookie) override;

    void Reset(TCookie cookie, NChunkPools::TChunkStripePtr stripe, NChunkPools::TInputChunkMappingPtr mapping) override;

    void Finish() override;

    bool IsFinished() const override;

protected:
    // NB: Underlying input is owned by the owner of the adapter.
    NChunkPools::IPersistentChunkPoolInputPtr UnderlyingInput_;

    PHOENIX_DECLARE_POLYMORPHIC_TYPE(TChunkPoolInputAdapterBase, 0xfdab51a9);
};

////////////////////////////////////////////////////////////////////////////////

class TChunkPoolOutputAdapterBase
    : public virtual NChunkPools::IPersistentChunkPoolOutput
{
public:
    //! Used only for persistence.
    TChunkPoolOutputAdapterBase() = default;

    explicit TChunkPoolOutputAdapterBase(NChunkPools::IPersistentChunkPoolOutputPtr underlyingOutput);

    const TProgressCounterPtr& GetJobCounter() const override;
    const TProgressCounterPtr& GetDataWeightCounter() const override;
    const TProgressCounterPtr& GetRowCounter() const override;
    const TProgressCounterPtr& GetDataSliceCounter() const override;

    NChunkPools::TOutputOrderPtr GetOutputOrder() const override;

    i64 GetLocality(NNodeTrackerClient::TNodeId nodeId) const override;

    NTableClient::TChunkStripeStatisticsVector GetApproximateStripeStatistics() const override;

    TCookie Extract(NNodeTrackerClient::TNodeId nodeId) override;

    NChunkPools::TChunkStripeListPtr GetStripeList(TCookie cookie) override;

    bool IsCompleted() const override;

    int GetStripeListSliceCount(TCookie cookie) const override;

    void Completed(TCookie cookie, const TCompletedJobSummary& jobSummary) override;
    void Failed(TCookie cookie) override;
    void Aborted(TCookie cookie, NScheduler::EAbortReason reason) override;
    void Lost(TCookie cookie) override;

    bool IsSplittable(NChunkPools::TOutputCookie cookie) const override;

    DECLARE_SIGNAL_OVERRIDE(void(NChunkClient::TInputChunkPtr, std::any tag), ChunkTeleported);

    DECLARE_SIGNAL_OVERRIDE(void(), Completed);
    DECLARE_SIGNAL_OVERRIDE(void(), Uncompleted);

protected:
    NChunkPools::IPersistentChunkPoolOutputPtr UnderlyingOutput_;

    PHOENIX_DECLARE_POLYMORPHIC_TYPE(TChunkPoolOutputAdapterBase, 0xfaeae6aa);
};

////////////////////////////////////////////////////////////////////////////////

class TChunkPoolAdapterBase
    : public virtual NChunkPools::IPersistentChunkPool
    , public TChunkPoolInputAdapterBase
    , public TChunkPoolOutputAdapterBase
{
public:
    //! Used only for persistence.
    TChunkPoolAdapterBase() = default;

    explicit TChunkPoolAdapterBase(NChunkPools::IPersistentChunkPoolPtr underlyingPool);

    PHOENIX_DECLARE_POLYMORPHIC_TYPE(TChunkPoolAdapterBase, 0x61d3d5b0);
};

////////////////////////////////////////////////////////////////////////////////

NChunkPools::IPersistentChunkPoolInputPtr CreateIntermediateLivePreviewAdapter(
    NChunkPools::IPersistentChunkPoolInputPtr chunkPoolInput,
    ITaskHost* taskHost);

NChunkPools::IPersistentChunkPoolInputPtr CreateTaskUpdatingAdapter(
    NChunkPools::IPersistentChunkPoolInputPtr chunkPoolInput,
    TTask* task);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent::NControllers
