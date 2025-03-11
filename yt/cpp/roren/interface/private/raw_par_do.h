#pragma once

#include "../fns.h"

#include "fn_attributes_ops.h"
#include "raw_data_flow.h"
#include "raw_transform.h"
#include "row_vtable.h"
#include "save_loadable_pointer_wrapper.h"
#include <yt/cpp/roren/library/unordered_invoker/unordered_invoker.h>

#include <type_traits>
#include <util/ysaveload.h>
#include <util/generic/function.h>

#include <optional>
#include <span>
#include <type_traits>

namespace NRoren::NPrivate {

////////////////////////////////////////////////////////////////////////////////

template <typename T>
concept CDoFn = std::default_initializable<T> && std::derived_from<T, IDoFn<typename T::TInputRow, typename T::TOutputRow>>;

template <CDoFn TFunction>
class TRawParDo
    : public IRawParDo
{
public:
    using TInputRow = std::decay_t<typename TFunction::TInputRow>;
    using TOutputRow = typename TFunction::TOutputRow;
    static constexpr bool IsMove = CMoveRow<typename TFunction::TInputRow>;

public:
    TRawParDo() = default;

    TRawParDo(TIntrusivePtr<TFunction> func)
        : Func_(std::move(func))
        , InputTag_(TTypeTag<TInputRow>("par-do-input"))
        , OutputTags_(Func_->GetOutputTags())
        , FnAttributes_(Func_->GetDefaultAttributes())
    {
        TFnAttributesOps::SetIsMove(FnAttributes_, IsMove);
        if constexpr (std::is_same_v<TOutputRow, void>) {
            Y_ABORT_UNLESS(OutputTags_.size() == 0);
        } else if constexpr (std::is_same_v<TOutputRow, TMultiRow>) {
            // Do nothing
        } else {
            Y_ABORT_UNLESS(OutputTags_.size() == 1, "OutputTags_.size() == %ld", OutputTags_.size());
        }
    }

    void Start(const IExecutionContextPtr& context, const std::vector<IRawOutputPtr>& outputs) override
    {
        if constexpr (std::is_same_v<typename TFunction::TOutputRow, void>) {
            // Do nothing
        } else if constexpr (std::is_same_v<typename TFunction::TOutputRow, TMultiRow>) {
            MultiOutput_.emplace(OutputTags_, outputs);
        } else {
            Y_ABORT_UNLESS(outputs.size() == 1);
            SingleOutput_ = outputs[0];
        }

        Func_->SetExecutionContext(context);
        Func_->Start(GetOutput());
    }

    void Do(const void* rows, int count) override
    {
        const TInputRow* current = static_cast<const TInputRow*>(rows);
        const TInputRow* end = current + count;

        for (; current < end; ++current) {
            if constexpr (IsMove) {
                TInputRow copy(*current);
                InvokeUnordered<GetMemberPointerClass<decltype(&TFunction::Do)>>(&TFunction::Do, Func_.Get(), std::move(copy), GetOutput());
            } else {
                InvokeUnordered<GetMemberPointerClass<decltype(&TFunction::Do)>>(&TFunction::Do, Func_.Get(), *current, GetOutput());
            }
        }
    }

    void MoveDo(void* rows, int count) override
    {
        TInputRow* current = static_cast<TInputRow*>(rows);
        TInputRow* end = current + count;

        for (; current < end; ++current) {
            InvokeUnordered<GetMemberPointerClass<decltype(&TFunction::Do)>>(&TFunction::Do, Func_.Get(), std::move(*current), GetOutput());
        }
    }

    void Finish() override
    {
        Func_->Finish(GetOutput());
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
        return [] () -> IRawParDoPtr {
            return MakeIntrusive<TRawParDo>();
        };
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
    std::optional<TMultiOutput> MultiOutput_;
    IRawOutputPtr SingleOutput_;
    TDynamicTypeTag InputTag_;
    std::vector<TDynamicTypeTag> OutputTags_;
    TFnAttributes FnAttributes_;
};

////////////////////////////////////////////////////////////////////////////////

template <typename T>
concept CLambdaState = std::default_initializable<T>
    && requires (
        const T& state,
        std::span<const typename T::TInputRow> rows,
        std::span<typename T::TInputRow> movedRows,
        TOutput<typename T::TOutputRow>& output,
        IExecutionContext* executionContext
    ) {
        { state.Do(rows, output, executionContext) } -> std::same_as<void>;
        { state.MoveDo(movedRows, output, executionContext) } -> std::same_as<void>;
        { T::IsMove } -> std::convertible_to<bool>;
    };

template <CLambdaState TLambdaState>
class TLambdaRawParDo
    : public IRawParDo
{
public:
    using TInputRow = typename TLambdaState::TInputRow;
    using TOutputRow = typename TLambdaState::TOutputRow;

    TLambdaRawParDo() = default;
    TLambdaRawParDo(TLambdaState state, TFnAttributes fnAttributes)
        : State_(std::move(state))
        , FnAttributes_(std::move(fnAttributes))
    {
        FnAttributes_.SetIsPure();
        TFnAttributesOps::SetIsMove(FnAttributes_, TLambdaState::IsMove);
    }

    void Start(const IExecutionContextPtr& context, const std::vector<IRawOutputPtr>& outputs) override
    {
        ExecutionContext_ = context;
        if constexpr (!std::same_as<TOutputRow, void>) {
            Y_ABORT_UNLESS(outputs.size() == 1);
            Output_ = outputs[0];
        }
    }

    void Do(const void* rows, int count) override
    {
        const TInputRow* begin = static_cast<const TInputRow*>(rows);
        const TInputRow* end = begin + count;
        State_.Do(std::span(begin, end), GetOutput(), ExecutionContext_.Get());
    }

    void MoveDo(void* rows, int count) override
    {
        TInputRow* begin = static_cast<TInputRow*>(rows);
        TInputRow* end = begin + count;
        State_.MoveDo(std::span(begin, end), GetOutput(), ExecutionContext_.Get());
    }

    void Finish() override
    {
        Output_ = nullptr;
    }

    [[nodiscard]] TDefaultFactoryFunc GetDefaultFactory() const override
    {
        return [] () -> IRawParDoPtr {
            return ::MakeIntrusive<TLambdaRawParDo>();
        };
    }

    [[nodiscard]] std::vector<TDynamicTypeTag> GetInputTags() const override
    {
        return {TTypeTag<TInputRow>("lambda-par-do-input")};
    }

    [[nodiscard]] std::vector<TDynamicTypeTag> GetOutputTags() const override
    {
        if constexpr (std::same_as<TOutputRow, void>) {
            return {};
        } else {
            return {TTypeTag<TOutputRow>("lambda-par-do-output")};
        }
    }

    [[nodiscard]] const TFnAttributes& GetFnAttributes() const override
    {
        return FnAttributes_;
    }

private:
    auto& GetOutput()
    {
        if constexpr (std::same_as<TOutputRow, void>) {
            return VoidOutput;
        } else {
            return *Output_->Upcast<TOutputRow>();
        }
    }

    TLambdaState State_;
    TFnAttributes FnAttributes_;

    Y_SAVELOAD_DEFINE_OVERRIDE(
        State_,
        FnAttributes_
    );

    IExecutionContextPtr ExecutionContext_;
    IRawOutputPtr Output_;
};

////////////////////////////////////////////////////////////////////////////////

template <typename TInputRow_, typename TOutputRow_, typename... TArgs>
class TLambdaState
{
public:
    using TInputRow = std::decay_t<TInputRow_>;
    using TOutputRow = TOutputRow_;
    using TCallback = void(*)(TInputRow_, NRoren::TOutput<TOutputRow>&, IExecutionContext*, TArgs...);

    static constexpr bool IsMove = CMoveRow<TInputRow_>;

public:
    TLambdaState() = default;

    TLambdaState(TCallback callback, std::decay_t<TArgs>... args)
        : Args_{TDummy{}, std::move(args)...}
        , Callback_(std::move(callback))
    { }

    void Do(std::span<const TInputRow> rows, NRoren::TOutput<TOutputRow>& output, IExecutionContext* ctx) const
    {
        for (const auto& row : rows) {
            if constexpr (IsMove) {
                auto copy = row;
                RunCallback(std::move(copy), output, ctx);
            } else {
                RunCallback(row, output, ctx);
            }
        }
    }

    void MoveDo(std::span<TInputRow> rows, NRoren::TOutput<TOutputRow>& output, IExecutionContext* ctx) const
    {
        for (auto& row : rows) {
            RunCallback(std::move(row), output, ctx);
        }
    }

    Y_SAVELOAD_DEFINE(Args_, NPrivate::SaveLoadablePointer(Callback_));

private:
    void RunCallback(TInputRow_ row, NRoren::TOutput<TOutputRow_>& output, IExecutionContext* ctx) const
    {
        std::apply(
            [&] (TDummy, TArgs... args) {
                Callback_(std::forward<TInputRow_>(row), output, ctx, std::forward<TArgs>(args)...);
            },
            Args_);
    }

    struct TDummy {
        void Save(IOutputStream*) const {}
        void Load(IInputStream*) {}
    };

    std::tuple<TDummy, std::decay_t<TArgs>...> Args_;
    TCallback Callback_;
};

////////////////////////////////////////////////////////////////////////////////

template <CDoFn TDoFn>
IRawParDoPtr MakeRawParDo(TIntrusivePtr<TDoFn> doFn)
{
    return MakeIntrusive<TRawParDo<TDoFn>>(doFn);
}

template <CLambdaState TLambdaState>
IRawParDoPtr MakeRawParDo(TLambdaState lambdaState, TFnAttributes fnAttributes = {})
{
    return MakeIntrusive<TLambdaRawParDo<TLambdaState>>(std::move(lambdaState), std::move(fnAttributes));
}

// For test purposes only
template <typename TInputRow, typename TOutputRow>
IRawParDoPtr MakeRawParDo(void(*doFn)(TInputRow, NRoren::TOutput<TOutputRow>&), TFnAttributes fnAttributes = {})
{
    auto casted = SaveLoadablePointer(doFn);
    return NPrivate::MakeRawParDo(
        TLambdaState(
            +[] (TInputRow input, NRoren::TOutput<TOutputRow>& output, IExecutionContext*, decltype(casted) callback) {
                callback.Value(std::forward<TInputRow>(input), output);
            },
            casted),
        std::move(fnAttributes));
}

////////////////////////////////////////////////////////////////////////////////

IRawParDoPtr MakeRawIdComputation(TRowVtable rowVtable);

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren::NPrivate
