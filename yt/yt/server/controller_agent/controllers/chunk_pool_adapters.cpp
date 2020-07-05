#include "chunk_pool_adapters.h"

#include "task.h"

#include <yt/ytlib/chunk_client/input_data_slice.h>

namespace NYT::NControllerAgent::NControllers {

using namespace NChunkPools;

////////////////////////////////////////////////////////////////////////////////

TChunkPoolInputAdapterBase::TChunkPoolInputAdapterBase(IChunkPoolInputPtr underlyingInput)
    : UnderlyingInput_(std::move(underlyingInput))
{ }

IChunkPoolInput::TCookie TChunkPoolInputAdapterBase::AddWithKey(TChunkStripePtr stripe, TChunkStripeKey key)
{
    return UnderlyingInput_->AddWithKey(std::move(stripe), key);
}

IChunkPoolInput::TCookie TChunkPoolInputAdapterBase::Add(TChunkStripePtr stripe)
{
    return UnderlyingInput_->Add(std::move(stripe));
}

void TChunkPoolInputAdapterBase::Suspend(IChunkPoolInput::TCookie cookie)
{
    return UnderlyingInput_->Suspend(cookie);
}

void TChunkPoolInputAdapterBase::Resume(IChunkPoolInput::TCookie cookie)
{
    return UnderlyingInput_->Resume(cookie);
}

void TChunkPoolInputAdapterBase::Reset(IChunkPoolInput::TCookie cookie, TChunkStripePtr stripe, TInputChunkMappingPtr mapping)
{
    return UnderlyingInput_->Reset(cookie, std::move(stripe), std::move(mapping));
}

void TChunkPoolInputAdapterBase::Finish()
{
    return UnderlyingInput_->Finish();
}

bool TChunkPoolInputAdapterBase::IsFinished() const
{
    return UnderlyingInput_->IsFinished();
}

void TChunkPoolInputAdapterBase::Persist(const TPersistenceContext& context)
{
    using NYT::Persist;

    Persist(context, UnderlyingInput_);
}

////////////////////////////////////////////////////////////////////////////////

class TIntermediateLivePreviewAdapter
    : public TChunkPoolInputAdapterBase
{
public:
    TIntermediateLivePreviewAdapter() = default;

    TIntermediateLivePreviewAdapter(IChunkPoolInputPtr chunkPoolInput, ITaskHost* taskHost)
        : TChunkPoolInputAdapterBase(std::move(chunkPoolInput))
        , TaskHost_(taskHost)
    { }

    virtual TCookie AddWithKey(TChunkStripePtr stripe, TChunkStripeKey key) override
    {
        YT_VERIFY(!stripe->DataSlices.empty());
        for (const auto& dataSlice : stripe->DataSlices) {
            auto chunk = dataSlice->GetSingleUnversionedChunkOrThrow();
            TaskHost_->AttachToIntermediateLivePreview(chunk->ChunkId());
        }
        return TChunkPoolInputAdapterBase::AddWithKey(std::move(stripe), key);
    }

    virtual TCookie Add(TChunkStripePtr stripe) override
    {
        return AddWithKey(stripe, TChunkStripeKey());
    }

    void Persist(const TPersistenceContext& context)
    {
        TChunkPoolInputAdapterBase::Persist(context);

        using NYT::Persist;

        Persist(context, TaskHost_);
    }

private:
    DECLARE_DYNAMIC_PHOENIX_TYPE(TIntermediateLivePreviewAdapter, 0x1241741a);

    ITaskHost* TaskHost_ = nullptr;
};

DEFINE_DYNAMIC_PHOENIX_TYPE(TIntermediateLivePreviewAdapter);

IChunkPoolInputPtr CreateIntermediateLivePreviewAdapter(
    IChunkPoolInputPtr chunkPoolInput,
    ITaskHost* taskHost)
{
    return New<TIntermediateLivePreviewAdapter>(std::move(chunkPoolInput), taskHost);
}

////////////////////////////////////////////////////////////////////////////////

class THintAddingAdapter
    : public TChunkPoolInputAdapterBase
{
public:
    THintAddingAdapter() = default;

    THintAddingAdapter(IChunkPoolInputPtr chunkPoolInput, TTask* task)
        : TChunkPoolInputAdapterBase(std::move(chunkPoolInput))
        , Task_(task)
    { }

    virtual TCookie AddWithKey(TChunkStripePtr stripe, TChunkStripeKey key) override
    {
        Task_->GetTaskHost()->AddTaskLocalityHint(stripe, Task_);
        Task_->AddPendingHint();

        return TChunkPoolInputAdapterBase::AddWithKey(std::move(stripe), key);
    }

    virtual TCookie Add(TChunkStripePtr stripe) override
    {
        return AddWithKey(stripe, TChunkStripeKey());
    }

    void Persist(const TPersistenceContext& context)
    {
        TChunkPoolInputAdapterBase::Persist(context);

        using NYT::Persist;

        Persist(context, Task_);
    }

private:
    DECLARE_DYNAMIC_PHOENIX_TYPE(THintAddingAdapter, 0x1fe32cba);

    TTask* Task_ = nullptr;
};

DEFINE_DYNAMIC_PHOENIX_TYPE(THintAddingAdapter);

IChunkPoolInputPtr CreateHintAddingAdapter(
    IChunkPoolInputPtr chunkPoolInput,
    TTask* task)
{
    return New<THintAddingAdapter>(std::move(chunkPoolInput), task);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent::NControllers
