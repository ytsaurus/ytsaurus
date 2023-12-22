#pragma once

#include "raw_transform.h"
#include "save_loadable_pointer_wrapper.h"

#include "../output.h"

#include <yt/cpp/roren/interface/fwd.h>
#include <yt/cpp/roren/interface/fns.h>
#include <yt/cpp/roren/interface/type_tag.h>
#include <yt/cpp/roren/interface/private/row_vtable.h>

namespace NRoren::NPrivate {

////////////////////////////////////////////////////////////////////////////////

template <typename TFunction>
class TRawStatefulParDo
    : public IRawStatefulParDo
{
    using TInputRow = typename TFunction::TInputRow;
    using TOutputRow = typename TFunction::TOutputRow;
    using TState = typename TFunction::TState;
    using TKey = typename TInputRow::TKey;

public:
    TRawStatefulParDo() = default;

    TRawStatefulParDo(TIntrusivePtr<TFunction> func, const TRowVtable inputVtable, std::vector<TDynamicTypeTag> outputTags, TFnAttributes fnAttributes)
        : Func_(std::move(func))
        , InputTag_("stateful-do-fn-input", inputVtable)
        , OutputTags_(std::move(outputTags))
        , FnAttributes_(std::move(fnAttributes))
    { }

    void Start(const IExecutionContextPtr& context, IRawStateStorePtr rawStateMap, const std::vector<IRawOutputPtr>& outputs) override
    {
        if constexpr (std::is_same_v<typename TFunction::TOutputRow, void>) {
            // Do nothing
        } else if constexpr (std::is_same_v<typename TFunction::TOutputRow, TMultiRow>) {
            MultiOutput_.emplace(OutputTags_, outputs);
        } else {
            Y_ABORT_UNLESS(outputs.size() == 1);
            SingleOutput_ = outputs[0];
        }

        RawStateMap_ = std::move(rawStateMap);
        Func_->SetExecutionContext(context);
        Func_->Start(GetOutput());
    }

    void Do(const void* row, int count) override
    {
        const TInputRow* current = static_cast<const TInputRow*>(row);
        const TInputRow* end = current + count;

        if constexpr (std::is_base_of_v<IStatefulDoFn<TInputRow, TOutputRow, TState>, TFunction>) {
            for (; current < end; ++current) {
                const auto* key = &current->Key();
                auto* rawState = RawStateMap_->GetStateRaw(key);
                Func_->Do(*current, GetOutput(), *static_cast<TState*>(rawState));
            }
        } else {
            static_assert(TDependentFalse<TFunction>);
        }
    }

    void Finish() override
    {
        Func_->Finish(GetOutput(), *RawStateMap_->Upcast<TKey, TState>());
    }

    void Save(IOutputStream* output) const override
    {
        ::Save(output, InputTag_);
        ::Save(output, OutputTags_);
        ::Save(output, FnAttributes_);

        static_cast<const IFnBase*>(Func_.Get())->Save(output);
    }

    void Load(IInputStream* input) override
    {
        ::Load(input, InputTag_);
        ::Load(input, OutputTags_);
        ::Load(input, FnAttributes_);

        static_cast<IFnBase*>(Func_.Get())->Load(input);
    }

    [[nodiscard]] std::vector<TDynamicTypeTag> GetInputTags() const override
    {
        return {InputTag_};
    }

    [[nodiscard]] std::vector<TDynamicTypeTag> GetOutputTags() const override
    {
        return OutputTags_;
    }

    [[nodiscard]] const TFnAttributes& GetFnAttributes() const override
    {
        return FnAttributes_;
    }

    [[nodiscard]] TDefaultFactoryFunc GetDefaultFactory() const override
    {
        return [] () -> IRawStatefulParDoPtr {
            return MakeIntrusive<TRawStatefulParDo>();
        };
    }

    [[nodiscard]] TRowVtable GetStateVtable() const override
    {
        return MakeRowVtable<TState>();
    }

private:
    auto& GetOutput()
    {
        if constexpr (std::is_same_v<TOutputRow, void>) {
            return VoidOutput;
        } else if constexpr (std::is_same_v<TOutputRow, TMultiRow>) {
            return *MultiOutput_;
        } else {
            return *SingleOutput_->Upcast<TOutputRow>();
        }
    }

private:
    TIntrusivePtr<TFunction> Func_ = MakeIntrusive<TFunction>();
    TDynamicTypeTag InputTag_;
    std::vector<TDynamicTypeTag> OutputTags_;
    TFnAttributes FnAttributes_;

    std::optional<TMultiOutput> MultiOutput_;
    IRawOutputPtr SingleOutput_;
    IRawStateStorePtr RawStateMap_;
};

template <typename TFunction>
IRawStatefulParDoPtr MakeRawStatefulParDo(TIntrusivePtr<TFunction> fn, TFnAttributes fnAttributes)
{
    TRowVtable rowVtable = MakeRowVtable<typename TFunction::TInputRow>();
    return ::MakeIntrusive<TRawStatefulParDo<TFunction>>(fn, rowVtable, fn->GetOutputTags(), std::move(fnAttributes));
}

////////////////////////////////////////////////////////////////////////////////

class TLambdaStatefulParDo
    : public IRawStatefulParDo
{
public:
    enum class EWrapperType : ui8
    {
        SingleOutputWrapper,
        MultiOutputWrapper,
    };

    using TWrapperFunction = void (TLambdaStatefulParDo*, const void*, int);

    template <typename TInputRow, typename TOutputRow, typename TState>
    using TUnderlyingFunction = void (*)(const TInputRow&, TOutput<TOutputRow>&, TState&);

public:
    TLambdaStatefulParDo() = default;
    TLambdaStatefulParDo(
        TWrapperFunction* wrapperFunction,
        void* underlyingFunction,
        TRowVtable inputRowVtable,
        std::vector<TDynamicTypeTag> outputTags,
        TRowVtable stateVtable,
        TFnAttributes fnAttributes);

    template <typename TInputRow, typename TOutputRow, typename TState>
    static TIntrusivePtr<TLambdaStatefulParDo> MakeIntrusive(
        TUnderlyingFunction<TInputRow, TOutputRow, TState> underlyingFunction,
        TFnAttributes fnAttributes)
    {
        return ::MakeIntrusive<TLambdaStatefulParDo>(
            &RawWrapperFunc<TInputRow, TOutputRow, TState>,
            reinterpret_cast<void*>(underlyingFunction),
            MakeRowVtable<TInputRow>(),
            MakeTags<TOutputRow>(),
            MakeRowVtable<TState>(),
            std::move(fnAttributes)
        );
    }

    void Start(const IExecutionContextPtr& context, IRawStateStorePtr stateStore, const std::vector<IRawOutputPtr>& outputs) override;
    void Do(const void* rows, int count) override;
    void Finish() override;

    [[nodiscard]] TDefaultFactoryFunc GetDefaultFactory() const override;

    [[nodiscard]] std::vector<TDynamicTypeTag> GetInputTags() const override;
    [[nodiscard]] std::vector<TDynamicTypeTag> GetOutputTags() const override;
    [[nodiscard]] const TFnAttributes& GetFnAttributes() const override;
    [[nodiscard]] TRowVtable GetStateVtable() const override;

private:
    template <typename TInputRow, typename TOutputRow, typename TState>
    static void RawWrapperFunc(TLambdaStatefulParDo* pThis, const void* rows, int count)
    {
        auto span = std::span(static_cast<const TInputRow*>(rows), count);
        auto underlyingFunction = reinterpret_cast<TUnderlyingFunction<TInputRow, TOutputRow, TState>>(pThis->UnderlyingFunction_);
        TOutput<TOutputRow>* output;

        if constexpr (std::is_same_v<TOutputRow, void>) {
            output = &VoidOutput;
        } else {
            output = pThis->SingleOutput_->Upcast<TOutputRow>();
        }

        for (const auto& row : span) {
            const auto* key = &row.Key();
            auto* rawState = pThis->StateStore_->GetStateRaw(key);
            TState* state = static_cast<TState*>(rawState);
            underlyingFunction(row, *output, *state);
        }
    }

    template <typename TOutput>
    static std::vector<TDynamicTypeTag> MakeTags()
    {
        if constexpr (std::is_same_v<TOutput, void>) {
            return {};
        } else {
            static_assert(!CMultiRow<TOutput>);
            return std::vector<TDynamicTypeTag>{TTypeTag<TOutput>("stateful-map-output")};
        }
    }

private:
    TWrapperFunction* WrapperFunction_ = nullptr;
    void* UnderlyingFunction_ = nullptr;
    TDynamicTypeTag InputTag_;
    std::vector<TDynamicTypeTag> OutputTags_;
    TRowVtable StateVtable_;
    TFnAttributes FnAttributes_;

    Y_SAVELOAD_DEFINE_OVERRIDE(
        SaveLoadablePointer(WrapperFunction_),
        SaveLoadablePointer(UnderlyingFunction_),
        InputTag_,
        OutputTags_,
        StateVtable_,
        FnAttributes_
    );

    IRawOutputPtr SingleOutput_;
    IRawStateStorePtr StateStore_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren::NPrivate
