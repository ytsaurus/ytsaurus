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
concept CDoFn = std::derived_from<T, IDoFn<typename T::TInputRow, typename T::TOutputRow>>;

template <typename TFunc, typename TInputRow, typename TOutputRow>
class TFunctorDoFn
    : public IDoFn<TInputRow, TOutputRow>
{
public:
    TFunctorDoFn() = default;

    template <typename F>
    TFunctorDoFn(F&& func)
        : Func_(std::forward<F>(func))
    { }

    void Do(TDoFnInput<TInputRow> input, TOutput<TOutputRow>& output)
    {
        InvokeUnordered(
            Func_,
            std::forward<TDoFnInput<TInputRow>>(input),
            output,
            this->GetExecutionContext().Get());
    }

    NRoren::TFnAttributes GetDefaultAttributes() const override
    {
        NRoren::TFnAttributes attributes;
        attributes.SetIsPure();
        return attributes;
    }

    Y_SAVELOAD_DEFINE_OVERRIDE(Func_);

private:
    TFunc Func_ = {};
};

////////////////////////////////////////////////////////////////////////////////

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

    TRawParDo(NYT::TIntrusivePtr<TFunction> func)
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
            return NYT::New<TRawParDo>();
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
    NYT::TIntrusivePtr<TFunction> Func_ = NYT::New<TFunction>();
    std::optional<TMultiOutput> MultiOutput_;
    IRawOutputPtr SingleOutput_;
    TDynamicTypeTag InputTag_;
    std::vector<TDynamicTypeTag> OutputTags_;
    TFnAttributes FnAttributes_;
};

////////////////////////////////////////////////////////////////////////////////

template <CDoFn TDoFn>
IRawParDoPtr MakeRawParDo(NYT::TIntrusivePtr<TDoFn> doFn)
{
    return NYT::New<TRawParDo<TDoFn>>(doFn);
}

// For test purposes only.
template <typename TInputRow, typename TOutputRow>
IRawParDoPtr MakeRawParDo(void(*doFn)(TInputRow, NRoren::TOutput<TOutputRow>&))
{
    auto serializableFunctor = WrapToSerializableFunctor(doFn);
    using TDoFn = TFunctorDoFn<std::decay_t<decltype(serializableFunctor)>, TDoFnTemplateArgument<TInputRow>, TOutputRow>;
    return MakeRawParDo(NYT::New<TDoFn>(std::move(serializableFunctor)));
}

////////////////////////////////////////////////////////////////////////////////

IRawParDoPtr MakeRawIdComputation(TRowVtable rowVtable);

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren::NPrivate
