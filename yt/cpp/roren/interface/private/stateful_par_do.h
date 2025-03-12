#pragma once

#include "raw_transform.h"
#include "save_loadable_pointer_wrapper.h"

#include "../output.h"

#include <yt/cpp/roren/interface/fns.h>
#include <yt/cpp/roren/interface/fwd.h>
#include <yt/cpp/roren/interface/private/fn_attributes_ops.h>
#include <yt/cpp/roren/interface/private/row_vtable.h>
#include <yt/cpp/roren/interface/type_tag.h>

#include <yt/cpp/roren/library/unordered_invoker/unordered_invoker.h>

namespace NRoren::NPrivate {

////////////////////////////////////////////////////////////////////////////////

template <typename T>
concept CStatefulDoFn = std::default_initializable<T>
    && std::derived_from<T, IStatefulDoFn<typename T::TInputRow, typename T::TOutputRow, typename T::TState>>;

template <typename TFunc, typename TInputRow, typename TOutputRow, typename TState>
class TFunctorStatefulDoFn final
    : public IStatefulDoFn<TInputRow, TOutputRow, TState>
{
public:
    TFunctorStatefulDoFn() = default;

    template <typename F>
    TFunctorStatefulDoFn(F&& func)
        : Func_(std::forward<F>(func))
    { }

    void Do(TDoFnInput<TInputRow> input, TOutput<TOutputRow>& output, TState& state)
    {
        InvokeUnordered(
            Func_,
            std::forward<TDoFnInput<TInputRow>>(input),
            output,
            state,
            this->GetExecutionContext().Get());
    }

    Y_SAVELOAD_DEFINE_OVERRIDE(Func_);

private:
    TFunc Func_ = {};
};

template <CStatefulDoFn TFunction>
class TRawStatefulParDo
    : public IRawStatefulParDo
{
    using TInputRow = std::decay_t<typename TFunction::TInputRow>;
    using TOutputRow = typename TFunction::TOutputRow;
    using TState = typename TFunction::TState;
    using TKey = typename TInputRow::TKey;
    static constexpr bool IsMove = CMoveRow<typename TFunction::TInputRow>;

public:
    TRawStatefulParDo() = default;

    TRawStatefulParDo(TIntrusivePtr<TFunction> func)
        : Func_(std::move(func))
        , InputTag_("stateful-do-fn-input", MakeRowVtable<TInputRow>())
        , OutputTags_(Func_->GetOutputTags())
    {
        TFnAttributesOps::SetIsMove(FnAttributes_, IsMove);
    }

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

    void Do(const void* rows, int count) override
    {
        const TInputRow* current = static_cast<const TInputRow*>(rows);
        const TInputRow* end = current + count;

        for (; current < end; ++current) {
            const auto* key = &current->Key();
            auto* rawState = RawStateMap_->GetStateRaw(key);
            if constexpr (IsMove) {
                TInputRow copy(*current);
                InvokeUnordered<GetMemberPointerClass<decltype(&TFunction::Do)>>(
                    &TFunction::Do,
                    Func_.Get(),
                    std::move(copy),
                    GetOutput(),
                    *static_cast<TState*>(rawState));
            } else {
                InvokeUnordered<GetMemberPointerClass<decltype(&TFunction::Do)>>(
                    &TFunction::Do,
                    Func_.Get(),
                    *current,
                    GetOutput(),
                    *static_cast<TState*>(rawState));
            }
        }
    }

    void MoveDo(void* rows, int count) override
    {
        TInputRow* current = static_cast<TInputRow*>(rows);
        TInputRow* end = current + count;

        for (; current < end; ++current) {
            const auto* key = &current->Key();
            auto* rawState = RawStateMap_->GetStateRaw(key);
            InvokeUnordered<GetMemberPointerClass<decltype(&TFunction::Do)>>(
                &TFunction::Do,
                Func_.Get(),
                std::move(*current),
                GetOutput(),
                *static_cast<TState*>(rawState));
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
IRawStatefulParDoPtr MakeRawStatefulParDo(TIntrusivePtr<TFunction> fn)
{
    return ::MakeIntrusive<TRawStatefulParDo<TFunction>>(fn);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren::NPrivate
