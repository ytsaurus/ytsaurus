#pragma once

#include "private.h"

#include "task_host.h"

#include <yt/server/chunk_pools/chunk_pool.h>

namespace NYT {
namespace NControllerAgent {

////////////////////////////////////////////////////////////////////////////////

class TChunkPoolInputAdapterBase
    : public NChunkPools::IChunkPoolInput
    , public NPhoenix::TFactoryTag<NPhoenix::TSimpleFactory>
{
public:
    TChunkPoolInputAdapterBase() = default;

    explicit TChunkPoolInputAdapterBase(NChunkPools::IChunkPoolInput* underlyingInput);

    virtual TCookie AddWithKey(NChunkPools::TChunkStripePtr stripe, NChunkPools::TChunkStripeKey key) override;

    virtual TCookie Add(NChunkPools::TChunkStripePtr stripe) override;

    virtual void Suspend(TCookie cookie) override;

    virtual void Resume(TCookie cookie) override;

    virtual void Reset(TCookie cookie, NChunkPools::TChunkStripePtr stripe, TInputChunkMappingPtr mapping) override;

    virtual void Finish() override;

    void Persist(const TPersistenceContext& context);

private:
    // NB: Underlying input is owned by the owner of the adapter.
    NChunkPools::IChunkPoolInput* UnderlyingInput_ = nullptr;
};

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<NChunkPools::IChunkPoolInput> CreateIntermediateLivePreviewAdapter(
    NChunkPools::IChunkPoolInput* chunkPoolInput,
    ITaskHost* taskHost);

std::unique_ptr<NChunkPools::IChunkPoolInput> CreateHintAddingAdapter(
    NChunkPools::IChunkPoolInput* chunkPoolInput,
    TTask* task);

////////////////////////////////////////////////////////////////////////////////

} // namespace NControllerAgent
} // namespace NYT
