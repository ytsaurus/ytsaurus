#pragma once

#include "private.h"

#include "task_host.h"

#include <yt/yt/server/lib/chunk_pools/chunk_pool.h>

namespace NYT::NControllerAgent::NControllers {

////////////////////////////////////////////////////////////////////////////////

class TChunkPoolInputAdapterBase
    : public virtual NChunkPools::IPersistentChunkPoolInput
    , public virtual NPhoenix::TFactoryTag<NPhoenix::TSimpleFactory>
{
public:
    TChunkPoolInputAdapterBase() = default;

    explicit TChunkPoolInputAdapterBase(NChunkPools::IPersistentChunkPoolInputPtr underlyingInput);

    virtual TCookie AddWithKey(NChunkPools::TChunkStripePtr stripe, NChunkPools::TChunkStripeKey key) override;

    virtual TCookie Add(NChunkPools::TChunkStripePtr stripe) override;

    virtual void Suspend(TCookie cookie) override;

    virtual void Resume(TCookie cookie) override;

    virtual void Reset(TCookie cookie, NChunkPools::TChunkStripePtr stripe, NChunkPools::TInputChunkMappingPtr mapping) override;

    virtual void Finish() override;

    virtual bool IsFinished() const override;

    void Persist(const TPersistenceContext& context) override;

protected:
    // NB: Underlying input is owned by the owner of the adapter.
    NChunkPools::IPersistentChunkPoolInputPtr UnderlyingInput_;
};

////////////////////////////////////////////////////////////////////////////////

class TChunkPoolOutputAdapterBase
    : public virtual NChunkPools::IPersistentChunkPoolOutput
    , public virtual NPhoenix::TFactoryTag<NPhoenix::TSimpleFactory>
{
public:
    //! Used only for persistence.
    TChunkPoolOutputAdapterBase() = default;

    explicit TChunkPoolOutputAdapterBase(NChunkPools::IPersistentChunkPoolOutputPtr underlyingOutput);

    virtual const TProgressCounterPtr& GetJobCounter() const override;
    virtual const TProgressCounterPtr& GetDataWeightCounter() const override;
    virtual const TProgressCounterPtr& GetRowCounter() const override;
    virtual const TProgressCounterPtr& GetDataSliceCounter() const override;

    virtual NChunkPools::TOutputOrderPtr GetOutputOrder() const override;

    virtual i64 GetLocality(NNodeTrackerClient::TNodeId nodeId) const override;

    virtual NTableClient::TChunkStripeStatisticsVector GetApproximateStripeStatistics() const override;

    virtual TCookie Extract(NNodeTrackerClient::TNodeId nodeId) override;

    virtual NChunkPools::TChunkStripeListPtr GetStripeList(TCookie cookie) override;

    virtual bool IsCompleted() const override;

    virtual int GetStripeListSliceCount(TCookie cookie) const override;

    virtual void Completed(TCookie cookie, const TCompletedJobSummary& jobSummary) override;
    virtual void Failed(TCookie cookie) override;
    virtual void Aborted(TCookie cookie, NScheduler::EAbortReason reason) override;
    virtual void Lost(TCookie cookie) override;

    virtual bool IsSplittable(NChunkPools::TOutputCookie cookie) const override;

    void Persist(const TPersistenceContext& context) override;

    DECLARE_SIGNAL_OVERRIDE(void(NChunkClient::TInputChunkPtr, std::any tag), ChunkTeleported);

    DECLARE_SIGNAL_OVERRIDE(void(), Completed);
    DECLARE_SIGNAL_OVERRIDE(void(), Uncompleted);

protected:
    NChunkPools::IPersistentChunkPoolOutputPtr UnderlyingOutput_;
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

    void Persist(const TPersistenceContext& context) override;
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
