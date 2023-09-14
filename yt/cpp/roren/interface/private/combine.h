#pragma once

#include "raw_data_flow.h"
#include "raw_transform.h"

namespace NRoren::NPrivate {

////////////////////////////////////////////////////////////////////////////////

template <typename TCombineFn>
class TRawCombine
    : public IRawCombine
{
public:
    using TInputRow = typename TCombineFn::TInputRow;
    using TAccumRow = typename TCombineFn::TAccumRow;
    using TOutputRow = typename TCombineFn::TOutputRow;

public:
    TRawCombine(ERawTransformType type, ::TIntrusivePtr<TCombineFn> combineFn, TRowVtable inputVtable, TRowVtable outputVtable)
        : IRawCombine(type)
        , CombineFn_(std::move(combineFn))
        , InputVtable_(std::move(inputVtable))
        , OutputVtable_(std::move(outputVtable))
    { }

    void CreateAccumulator(void* accum) override
    {
        *static_cast<TAccumRow*>(accum) = CombineFn_->CreateAccumulator();
    }

    void AddInput(void* accum, const void* input) override
    {
        CombineFn_->AddInput(static_cast<TAccumRow*>(accum), *static_cast<const TInputRow*>(input));
    }

    void MergeAccumulators(void* result, const IRawInputPtr& accums) override
    {
        *static_cast<TAccumRow*>(result) = CombineFn_->MergeAccumulators(*accums->Upcast<TAccumRow>());
    }

    void ExtractOutput(void* output, const void* accum) override
    {
        *static_cast<TOutputRow*>(output) = CombineFn_->ExtractOutput(*static_cast<const TAccumRow*>(accum));
    }

    TRowVtable GetInputVtable() const override
    {
        return InputVtable_;
    }

    TRowVtable GetAccumVtable() const override
    {
        return MakeRowVtable<TAccumRow>();
    }

    TRowVtable GetOutputVtable() const override
    {
        return OutputVtable_;
    }

    IRawCoderPtr GetAccumCoder() const override
    {
        return MakeDefaultRawCoder<TAccumRow>();
    }

    std::vector<TDynamicTypeTag> GetInputTags() const override
    {
        return {TDynamicTypeTag{"combine-per-key-input-0", InputVtable_}};
    }

    std::vector<TDynamicTypeTag> GetOutputTags() const override
    {
        return {TDynamicTypeTag{"combine-per-key-output-0", OutputVtable_}};
    }

    void Start(const IExecutionContextPtr& context) override
    {
        CombineFn_->SetExecutionContext(context);
    }

private:
    [[nodiscard]] TDefaultFactoryFunc GetDefaultFactory() const override
    {
        if (GetType() == ERawTransformType::CombineGlobally) {
            return []() -> IRawCombinePtr {
                return ::MakeIntrusive<TRawCombine>(ERawTransformType::CombineGlobally, ::MakeIntrusive<TCombineFn>(), TRowVtable{}, TRowVtable{});
            };
        } else {
            Y_VERIFY(GetType() == ERawTransformType::CombinePerKey);
            return []() -> IRawCombinePtr {
                return ::MakeIntrusive<TRawCombine>(ERawTransformType::CombinePerKey, ::MakeIntrusive<TCombineFn>(), TRowVtable{}, TRowVtable{});
            };
        }
    }

    void Save(IOutputStream* stream) const override
    {
        CombineFn_->Save(stream);
        ::Save(stream, InputVtable_);
        ::Save(stream, OutputVtable_);
    }

    void Load(IInputStream* stream) override
    {
        CombineFn_->Load(stream);
        ::Load(stream, InputVtable_);
        ::Load(stream, OutputVtable_);
    }

private:
    ::TIntrusivePtr<ICombineFn<TInputRow, TAccumRow, TOutputRow>> CombineFn_;
    TRowVtable InputVtable_;
    TRowVtable OutputVtable_;
};

template <typename TCombineFn>
IRawCombinePtr MakeRawCombine(ERawTransformType type, ::TIntrusivePtr<TCombineFn> combineFn, TRowVtable inputVtable, TRowVtable outputVtable)
{
    return ::MakeIntrusive<TRawCombine<TCombineFn>>(type, std::move(combineFn), std::move(inputVtable), std::move(outputVtable));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren::NPrivate
