#pragma once

#include <yt/cpp/roren/interface/private/save_loadable_pointer_wrapper.h>
#include <yt/cpp/roren/interface/roren.h>

namespace NRoren {

////////////////////////////////////////////////////////////////////////////////

//! Transforms that batches input messages until it reaches flush limit.
///
/// Batch transform expects input of type
///   TRow
/// Batch transform returns PCollection of type
///   std::vector<TRow>
template <typename TRow>
TTransform<TRow, std::vector<TRow>> Batch(
    size_t flushSize,
    size_t (*sizeFn)(const TRow&) =  [] (const TRow&) -> size_t { return 1; });

////////////////////////////////////////////////////////////////////////////////

template <typename TRow>
class TBatchParDo : public NRoren::IDoFn<TRow, std::vector<TRow>>
{
public:
    using TSizeFn = size_t(*)(const TRow&);

    TBatchParDo() = default;

    TBatchParDo(size_t flushBatchSize, TSizeFn sizeFn)
        : FlushBatchSize_(flushBatchSize)
        , SizeFn_(sizeFn)
    { }

    void Start(NRoren::TOutput<std::vector<TRow>>&) override
    {
        Y_ENSURE(SizeFn_ != nullptr);
        BatchSize_ = 0;
        Batch_.clear();
    }

    void Do(const TRow& input, TOutput<std::vector<TRow>>& output) override
    {
        BatchSize_ += SizeFn_(input);
        Batch_.push_back(input);
        Flush(output, false);
    }

    void Finish(TOutput<std::vector<TRow>>& output) override
    {
        Flush(output, true);
    }

    Y_SAVELOAD_DEFINE_OVERRIDE(FlushBatchSize_, NPrivate::SaveLoadablePointer(SizeFn_));

private:
    void Flush(TOutput<std::vector<TRow>>& output, bool force)
    {
        if (BatchSize_ >= FlushBatchSize_ || force) {
            output.Add(std::move(Batch_));
            Batch_.clear();
            BatchSize_ = 0;
        }
    }

    std::vector<TRow> Batch_;
    size_t BatchSize_ = 0;
    size_t FlushBatchSize_ = 0;
    TSizeFn SizeFn_ = nullptr;
};

////////////////////////////////////////////////////////////////////////////////

template <typename TRow>
TTransform<TRow, std::vector<TRow>> Batch(
    size_t flushSize,
    size_t (*sizeFn)(const TRow&)
) {
    return MakeParDo<TBatchParDo<TRow>>(flushSize, sizeFn);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren

