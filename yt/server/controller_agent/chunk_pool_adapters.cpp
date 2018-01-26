#include "chunk_pool_adapters.h"

#include "task.h"

namespace NYT {
namespace NControllerAgent {

using namespace NChunkPools;

////////////////////////////////////////////////////////////////////////////////

TChunkPoolInputAdapterBase::TChunkPoolInputAdapterBase(IChunkPoolInput* underlyingInput)
    : UnderlyingInput_(underlyingInput)
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

void TChunkPoolInputAdapterBase::Reset(IChunkPoolInput::TCookie cookie, TChunkStripePtr stripe)
{
    return UnderlyingInput_->Reset(cookie, stripe);
}

void TChunkPoolInputAdapterBase::Finish()
{
    return UnderlyingInput_->Finish();
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

    TIntermediateLivePreviewAdapter(IChunkPoolInput* chunkPoolInput, ITaskHost* taskHost)
        : TChunkPoolInputAdapterBase(chunkPoolInput)
        , TaskHost_(taskHost)
    { }

    virtual TCookie AddWithKey(TChunkStripePtr stripe, TChunkStripeKey key) override
    {
        YCHECK(!stripe->DataSlices.empty());
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

std::unique_ptr<IChunkPoolInput> CreateIntermediateLivePreviewAdapter(
    IChunkPoolInput* chunkPoolInput,
    ITaskHost* taskHost)
{
    return std::make_unique<TIntermediateLivePreviewAdapter>(chunkPoolInput, taskHost);
}

////////////////////////////////////////////////////////////////////////////////

class THintAddingAdapter
    : public TChunkPoolInputAdapterBase
{
public:
    THintAddingAdapter() = default;

    THintAddingAdapter(IChunkPoolInput* chunkPoolInput, TTask* task)
        : TChunkPoolInputAdapterBase(chunkPoolInput)
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

std::unique_ptr<IChunkPoolInput> CreateHintAddingAdapter(
    IChunkPoolInput* chunkPoolInput,
    TTask* task)
{
    return std::make_unique<THintAddingAdapter>(chunkPoolInput, task);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NControllerAgent
} // namespace NYT
