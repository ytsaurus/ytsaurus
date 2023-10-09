#pragma once

#include "raw_transform.h"

#include "../output.h"

#include <yt/cpp/roren/interface/fwd.h>
#include <yt/cpp/roren/interface/fns.h>
#include <yt/cpp/roren/interface/type_tag.h>
#include <yt/cpp/roren/interface/private/row_vtable.h>

namespace NRoren::NPrivate {

////////////////////////////////////////////////////////////////////////////////

template <typename TFunction>
class TRawStatefulTimerParDo
    : public IRawStatefulTimerParDo
{
    using TInputRow = typename TFunction::TInputRow;
    using TOutputRow = typename TFunction::TOutputRow;
    using TState = typename TFunction::TState;
    using TKey = typename TInputRow::TKey;

public:
    TRawStatefulTimerParDo() = default;

    TRawStatefulTimerParDo(TIntrusivePtr<TFunction> func, const TRowVtable inputVtable, std::vector<TDynamicTypeTag> outputTags, TFnAttributes fnAttributes)
        : Func_(std::move(func))
        , InputTag_("stateful-timer-do-fn-input", inputVtable)
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

        if constexpr (std::is_base_of_v<IStatefulTimerDoFn<TInputRow, TOutputRow, TState>, TFunction>) {
            for (; current < end; ++current) {
                const auto* key = &current->Key();
                // TODO: we should keep pointer to upcasted state in our class.
                // here we should use member function of upcasted state.
                auto* rawState = RawStateMap_->GetStateRaw(key);
                Func_->Do(*current, GetOutput(), *static_cast<TState*>(rawState));
            }
        } else {
            static_assert(TDependentFalse<TFunction>);
        }
    }

    void OnTimer(const void* rawKey, const TTimer& timer) override
    {
        if constexpr (std::is_base_of_v<IStatefulTimerDoFn<TInputRow, TOutputRow, TState>, TFunction>) {
            const auto* key = static_cast<const typename TInputRow::TKey*>(rawKey);
            // TODO: we should keep pointer to upcasted state in our class.
            // here we should use member function of upcasted state.
            auto* rawState = RawStateMap_->GetStateRaw(key);
            Func_->OnTimer(*key, GetOutput(), *static_cast<TState*>(rawState), NRoren::TTimerContext{timer});
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

    [[nodiscard]] TDefaultFactoryFunc GetDefaultFactory() const override
    {
        return [] () -> IRawStatefulTimerParDoPtr {
            return MakeIntrusive<TRawStatefulTimerParDo>();
        };
    }

    [[nodiscard]] TRowVtable GetStateVtable() const override
    {
        return MakeRowVtable<TState>();
    }

    [[nodiscard]] const TFnAttributes& GetFnAttributes() const override
    {
        return FnAttributes_;
    }

    [[nodiscard]] const TString& GetFnId() const override
    {
        return Func_->GetFnId();
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
IRawStatefulTimerParDoPtr MakeRawStatefulTimerParDo(TIntrusivePtr<TFunction> fn, TFnAttributes fnAttributes)
{
    TRowVtable rowVtable = MakeRowVtable<typename TFunction::TInputRow>();
    return ::MakeIntrusive<TRawStatefulTimerParDo<TFunction>>(fn, rowVtable, fn->GetOutputTags(), std::move(fnAttributes));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren::NPrivate
