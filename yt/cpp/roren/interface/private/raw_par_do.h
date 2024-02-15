#pragma once

#include "../fns.h"

#include "raw_data_flow.h"
#include "raw_transform.h"
#include "row_vtable.h"
#include "save_loadable_pointer_wrapper.h"

#include <type_traits>
#include <util/ysaveload.h>
#include <util/generic/function.h>

#include <optional>
#include <span>
#include <type_traits>

namespace NRoren::NPrivate {

////////////////////////////////////////////////////////////////////////////////

template <typename TFunction>
class TRawParDo
    : public IRawParDo
{
public:
    using TInputRow = typename TFunction::TInputRow;
    using TOutputRow = typename TFunction::TOutputRow;

    static_assert(
        std::is_convertible_v<TFunction*, IDoFn<TInputRow, TOutputRow>*>,
        "Argument must be subclass of IDoFn");
    static_assert(std::is_default_constructible_v<TFunction>, "ParDo class must have default constructor");

public:
    TRawParDo() = default;

    TRawParDo(TIntrusivePtr<TFunction> func, const TRowVtable inputVtable, std::vector<TDynamicTypeTag> outputTags, TFnAttributes fnAttributes)
        : Func_(std::move(func))
        , InputTag_("par-do-input", inputVtable)
        , OutputTags_(std::move(outputTags))
        , FnAttributes_(std::move(fnAttributes))
    {
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

    void Do(const void* row, int count) override
    {
        const TInputRow* current = static_cast<const typename TFunction::TInputRow*>(row);
        const TInputRow* end = current + count;

        if constexpr (std::is_base_of_v<IDoFn<TInputRow, TOutputRow>, TFunction>) {
            for (; current < end; ++current) {
                Func_->Do(*current, GetOutput());
            }
        } else {
            static_assert(TDependentFalse<TFunction>);
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

class TLambda1RawParDo
    : public IRawParDo
{
public:
    enum class EWrapperType : ui8 {
        WrapperType1,
        WrapperType2,
        MultiOutputWrapper,
    };

    using TWrapperFunctionPtr = void (*)(TLambda1RawParDo*, const void*, int);

    template <typename TInputRow, typename TOutputRow>
    using TRawFunction1 = TOutputRow (*)(const TInputRow&);

    template <typename TInputRow, typename TOutputRow>
    using TRawFunction2 = void (*)(const TInputRow&, TOutput<TOutputRow>&);

public:
    TLambda1RawParDo() = default;
    TLambda1RawParDo(
        TWrapperFunctionPtr wrapperFunction,
        EWrapperType wrapperType,
        void* function,
        const TRowVtable& rowVtable,
        std::vector<TDynamicTypeTag> tags,
        TFnAttributes fnAttributes);

    template <typename TInput, typename TOutput>
    static TIntrusivePtr<TLambda1RawParDo> MakeIntrusive(TRawFunction1<TInput, TOutput> function, TFnAttributes fnAttributes)
    {
        fnAttributes.SetIsPure();
        return ::MakeIntrusive<TLambda1RawParDo>(
            &RawWrapper1Func<TInput, TOutput>,
            EWrapperType::WrapperType1,
            reinterpret_cast<void*>(function),
            MakeRowVtable<TInput>(),
            MakeTags<TOutput>(),
            std::move(fnAttributes)
        );
    }

    template <typename TInput, typename TOutput>
    static TIntrusivePtr<TLambda1RawParDo> MakeIntrusive(TRawFunction2<TInput, TOutput> function, TFnAttributes fnAttributes)
    {
        fnAttributes.SetIsPure();
        return ::MakeIntrusive<TLambda1RawParDo>(
            &RawWrapper2Func<TInput, TOutput>,
            EWrapperType::WrapperType2,
            reinterpret_cast<void*>(function),
            MakeRowVtable<TInput>(),
            MakeTags<TOutput>(),
            std::move(fnAttributes)
        );
    }

    template <typename TInput>
    static TIntrusivePtr<TLambda1RawParDo> MakeIntrusive(
        TRawFunction2<TInput, TMultiRow> function,
        std::vector<TDynamicTypeTag> tags,
        TFnAttributes fnAttributes)
    {
        return ::MakeIntrusive<TLambda1RawParDo>(
            &RawWrapperMultiOutputFunc<TInput>,
            EWrapperType::MultiOutputWrapper,
            reinterpret_cast<void*>(function),
            MakeRowVtable<TInput>(),
            std::move(tags),
            std::move(fnAttributes)
        );
    }

    void Start(const IExecutionContextPtr& context, const std::vector<IRawOutputPtr>& outputs) override;
    void Do(const void* row, int count) override;
    void Finish() override;

    [[nodiscard]] TDefaultFactoryFunc GetDefaultFactory() const override;

    [[nodiscard]] std::vector<TDynamicTypeTag> GetInputTags() const override;
    [[nodiscard]] std::vector<TDynamicTypeTag> GetOutputTags() const override;
    [[nodiscard]] const TFnAttributes& GetFnAttributes() const override;

private:
    template <typename TInputRow, typename TOutputRow>
    static void RawWrapper1Func(TLambda1RawParDo* pThis, const void* rows, int count)
    {
        auto span = std::span(static_cast<const TInputRow*>(rows), count);
        auto underlyingFunction = reinterpret_cast<TRawFunction1<TInputRow, TOutputRow>>(pThis->UnderlyingFunction_);

        for (const auto& row : span) {
            if constexpr (std::is_same_v<TOutputRow, void>) {
                underlyingFunction(row);
            } else {
                *static_cast<TOutputRow*>(pThis->RowHolder_.GetData()) = underlyingFunction(row);
                pThis->SingleOutput_->AddRaw(pThis->RowHolder_.GetData(), 1);
            }
        }
    }

    template <typename TInputRow, typename TOutputRow>
    static void RawWrapper2Func(TLambda1RawParDo* pThis, const void* rows, int count)
    {
        auto span = std::span(static_cast<const TInputRow*>(rows), count);
        TOutput<TOutputRow>* upcastedOutput;
        if constexpr (std::is_same_v<TOutputRow, void>) {
            upcastedOutput = &VoidOutput;
        } else {
            upcastedOutput = pThis->SingleOutput_->Upcast<TOutputRow>();
        }
        auto underlyingFunction = reinterpret_cast<TRawFunction2<TInputRow, TOutputRow>>(pThis->UnderlyingFunction_);

        for (const auto& row : span) {
            underlyingFunction(row, *upcastedOutput);
        }
    }

    template <typename TInputRow>
    static void RawWrapperMultiOutputFunc(TLambda1RawParDo* pThis, const void* rows, int count)
    {
        auto span = std::span(static_cast<const TInputRow*>(rows), count);
        auto underlyingFunction = reinterpret_cast<TRawFunction2<TInputRow, TMultiRow>>(pThis->UnderlyingFunction_);

        for (const auto& row : span) {
            underlyingFunction(row, *pThis->MultiOutput_);
        }
    }

    template <typename TOutput>
    static std::vector<TDynamicTypeTag> MakeTags()
    {
        if constexpr (std::is_same_v<TOutput, void>) {
            return {};
        } else {
            static_assert(!CMultiRow<TOutput>);
            return std::vector<TDynamicTypeTag>{TTypeTag<TOutput>("map-output")};
        }
    }

private:
    TWrapperFunctionPtr WrapperFunction_ = nullptr;
    EWrapperType WrapperType_;
    void* UnderlyingFunction_ = nullptr;
    TDynamicTypeTag InputTag_;
    std::vector<TDynamicTypeTag> OutputTags_;
    TFnAttributes FnAttributes_;

    Y_SAVELOAD_DEFINE_OVERRIDE(
        SaveLoadablePointer(WrapperFunction_),
        WrapperType_,
        SaveLoadablePointer(UnderlyingFunction_),
        InputTag_,
        OutputTags_,
        FnAttributes_
    );

    // Initialization of fields depends on the WrapperType_:
    // * MultiOutput_ is initialized for MultiOutputWrapper;
    // * SingleOutput_ is initialized for WrapperType1 and WrapperType2;
    // * RowHolder_ is initialized for WrapperType1;
    std::optional<TMultiOutput> MultiOutput_;
    IRawOutputPtr SingleOutput_;
    TRawRowHolder RowHolder_;
};

template <typename TDoFn>
IRawParDoPtr MakeRawParDo(TIntrusivePtr<TDoFn> doFn, const TRowVtable& inputVtable = MakeRowVtable<typename TDoFn::TInputRow>())
{
    return MakeIntrusive<TRawParDo<TDoFn>>(doFn, inputVtable, doFn->GetOutputTags(), doFn->GetDefaultAttributes());
}

template <typename TDoFn>
IRawParDoPtr MakeRawParDo(TIntrusivePtr<TDoFn> doFn, const TRowVtable& inputVtable, TFnAttributes fnAttributes)
{
    return MakeIntrusive<TRawParDo<TDoFn>>(doFn, inputVtable, doFn->GetOutputTags(), std::move(fnAttributes));
}

template <typename TInput, typename TOutput, typename F>
IRawParDoPtr MakeRawParDo(F&& doFn, TFnAttributes fnAttributes = {})
    requires(std::is_convertible_v<F, TOutput(*)(const TInput&)>)
{
    return TLambda1RawParDo::MakeIntrusive<TInput, TOutput>(doFn, std::move(fnAttributes));
}

////////////////////////////////////////////////////////////////////////////////

IRawParDoPtr MakeRawIdComputation(TRowVtable rowVtable);

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren::NPrivate
