#pragma once

#include "private.h"

#include "task_host.h"

#include <yt/server/lib/legacy_chunk_pools/chunk_pool.h>

namespace NYT::NControllerAgent::NLegacyControllers {

////////////////////////////////////////////////////////////////////////////////

class TChunkPoolInputAdapterBase
    : public NLegacyChunkPools::IChunkPoolInput
    , public NPhoenix::TFactoryTag<NPhoenix::TSimpleFactory>
{
public:
    TChunkPoolInputAdapterBase() = default;

    explicit TChunkPoolInputAdapterBase(NLegacyChunkPools::IChunkPoolInputPtr underlyingInput);

    virtual TCookie AddWithKey(NLegacyChunkPools::TChunkStripePtr stripe, NLegacyChunkPools::TChunkStripeKey key) override;

    virtual TCookie Add(NLegacyChunkPools::TChunkStripePtr stripe) override;

    virtual void Suspend(TCookie cookie) override;

    virtual void Resume(TCookie cookie) override;

    virtual void Reset(TCookie cookie, NLegacyChunkPools::TChunkStripePtr stripe, NLegacyChunkPools::TInputChunkMappingPtr mapping) override;

    virtual void Finish() override;

    virtual bool IsFinished() const override;

    void Persist(const TPersistenceContext& context);

private:
    // NB: Underlying input is owned by the owner of the adapter.
    NLegacyChunkPools::IChunkPoolInputPtr UnderlyingInput_;
};

////////////////////////////////////////////////////////////////////////////////

NLegacyChunkPools::IChunkPoolInputPtr CreateIntermediateLivePreviewAdapter(
    NLegacyChunkPools::IChunkPoolInputPtr chunkPoolInput,
    ITaskHost* taskHost);

NLegacyChunkPools::IChunkPoolInputPtr CreateHintAddingAdapter(
    NLegacyChunkPools::IChunkPoolInputPtr chunkPoolInput,
    TTask* task);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent::NLegacyControllers
