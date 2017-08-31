#include "chunk_pool_adapters.h"

#include "task.h"

namespace NYT {
namespace NControllerAgent {

using namespace NChunkPools;

////////////////////////////////////////////////////////////////////////////////

class TChunkPoolInputAdapterBase
    : public IChunkPoolInput
    , public NPhoenix::TFactoryTag<NPhoenix::TSimpleFactory>
{
public:
    TChunkPoolInputAdapterBase() = default;

    explicit TChunkPoolInputAdapterBase(IChunkPoolInput* underlyingInput)
        : UnderlyingInput_(underlyingInput)
    { }

    virtual TCookie Add(TChunkStripePtr stripe, TChunkStripeKey key) override
    {
        return UnderlyingInput_->Add(std::move(stripe), key);
    }

    virtual void Suspend(TCookie cookie) override
    {
        UnderlyingInput_->Suspend(cookie);
    }

    virtual void Resume(TCookie cookie, TChunkStripePtr stripe) override
    {
        UnderlyingInput_->Resume(cookie, std::move(stripe));
    }

    virtual void Finish() override
    {
        UnderlyingInput_->Finish();
    }

    void Persist(const TPersistenceContext& context)
    {
        using NYT::Persist;

        Persist(context, UnderlyingInput_);
    }

private:
    // NB: Underlying input is owned by the owner of the adapter.
    IChunkPoolInput* UnderlyingInput_ = nullptr;
};

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

    virtual TCookie Add(TChunkStripePtr stripe, TChunkStripeKey key) override
    {
        YCHECK(!stripe->DataSlices.empty());
        for (const auto& dataSlice : stripe->DataSlices) {
            auto chunk = dataSlice->GetSingleUnversionedChunkOrThrow();
            TaskHost_->AttachToIntermediateLivePreview(chunk->ChunkId());
        }
        return TChunkPoolInputAdapterBase::Add(std::move(stripe), key);
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

    virtual TCookie Add(TChunkStripePtr stripe, TChunkStripeKey key) override
    {
        Task_->GetTaskHost()->AddTaskLocalityHint(stripe, Task_);
        Task_->AddPendingHint();

        return TChunkPoolInputAdapterBase::Add(std::move(stripe), key);
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