#pragma once

#include "private.h"

#include "task_host.h"

#include <yt/server/lib/chunk_pools/chunk_pool.h>

namespace NYT::NControllerAgent {

////////////////////////////////////////////////////////////////////////////////

class TChunkPoolInputAdapterBase
    : public NChunkPools::IChunkPoolInput
    , public NPhoenix::TFactoryTag<NPhoenix::TSimpleFactory>
{
public:
    TChunkPoolInputAdapterBase() = default;

    explicit TChunkPoolInputAdapterBase(NChunkPools::IChunkPoolInputPtr underlyingInput);

    virtual TCookie AddWithKey(NChunkPools::TChunkStripePtr stripe, NChunkPools::TChunkStripeKey key) override;

    virtual TCookie Add(NChunkPools::TChunkStripePtr stripe) override;

    virtual void Suspend(TCookie cookie) override;

    virtual void Resume(TCookie cookie) override;

    virtual void Reset(TCookie cookie, NChunkPools::TChunkStripePtr stripe, NChunkPools::TInputChunkMappingPtr mapping) override;

    virtual void Finish() override;

    virtual bool IsFinished() const override;

    void Persist(const TPersistenceContext& context);

private:
    // NB: Underlying input is owned by the owner of the adapter.
    NChunkPools::IChunkPoolInputPtr UnderlyingInput_;
};

////////////////////////////////////////////////////////////////////////////////

NChunkPools::IChunkPoolInputPtr CreateIntermediateLivePreviewAdapter(
    NChunkPools::IChunkPoolInputPtr chunkPoolInput,
    ITaskHost* taskHost);

NChunkPools::IChunkPoolInputPtr CreateHintAddingAdapter(
    NChunkPools::IChunkPoolInputPtr chunkPoolInput,
    TTask* task);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent
