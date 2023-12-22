#include "raw_par_do.h"


namespace NRoren::NPrivate {

////////////////////////////////////////////////////////////////////////////////

TLambda1RawParDo::TLambda1RawParDo(
    TLambda1RawParDo::TWrapperFunctionPtr wrapperFunction,
    EWrapperType wrapperType,
    void* function,
    const TRowVtable& rowVtable,
    std::vector<TDynamicTypeTag> tags,
    TFnAttributes fnAttributes
)
    : WrapperFunction_(wrapperFunction)
    , WrapperType_(wrapperType)
    , UnderlyingFunction_(function)
    , InputTag_("lambda-par-do-input", rowVtable)
    , OutputTags_(std::move(tags))
    , FnAttributes_(std::move(fnAttributes))
{ }

void TLambda1RawParDo::Start(const IExecutionContextPtr&, const std::vector<IRawOutputPtr>& outputs)
{
    if (WrapperType_ == EWrapperType::MultiOutputWrapper) {
        MultiOutput_.emplace(OutputTags_, outputs);
    } else if (WrapperType_ == EWrapperType::WrapperType1 || WrapperType_ == EWrapperType::WrapperType2) {
        if (outputs.size() > 0) {
            Y_ABORT_UNLESS(outputs.size() == 1);

            SingleOutput_ = outputs[0];
            if (WrapperType_ == EWrapperType::WrapperType1) {
                RowHolder_ = TRawRowHolder(OutputTags_[0].GetRowVtable());
            }
        }
    }
}

void TLambda1RawParDo::Do(const void* rows, int count)
{
    WrapperFunction_(this, rows, count);
}

void TLambda1RawParDo::Finish()
{
    SingleOutput_ = nullptr;
    MultiOutput_.reset();
}

TLambda1RawParDo::TDefaultFactoryFunc TLambda1RawParDo::GetDefaultFactory() const
{
    return [] () -> IRawParDoPtr {
        return ::MakeIntrusive<TLambda1RawParDo>();
    };
}

std::vector<TDynamicTypeTag> TLambda1RawParDo::GetInputTags() const
{
    return {InputTag_};
}

std::vector<TDynamicTypeTag> TLambda1RawParDo::GetOutputTags() const
{
    return OutputTags_;
}

const TFnAttributes& TLambda1RawParDo::GetFnAttributes() const
{
    return FnAttributes_;
}

////////////////////////////////////////////////////////////////////////////////

class TRawIdComputation
    : public IRawParDo
{
public:
    TRawIdComputation() = default;

    explicit TRawIdComputation(TRowVtable rowVtable)
        : RowVtable_(rowVtable)
    { }

    void Start(const IExecutionContextPtr&, const std::vector<IRawOutputPtr>& outputs) override
    {
        Y_ABORT_UNLESS(outputs.size() == 1);
        Output_ = outputs[0];
    }

    void Do(const void* rows, int count) override
    {
        Output_->AddRaw(rows, count);
    }

    void Finish() override
    {
        Output_ = nullptr;
    }

    [[nodiscard]] TDefaultFactoryFunc GetDefaultFactory() const override
    {
        return MakeIntrusive;
    }

    std::vector<TDynamicTypeTag> GetInputTags() const override
    {
        return {{"id-input", RowVtable_}};
    }

    std::vector<TDynamicTypeTag> GetOutputTags() const override
    {
        return {{"id-output", RowVtable_}};
    }

    const TFnAttributes& GetFnAttributes() const override
    {
        static const TFnAttributes attributes;
        return attributes;
    }

private:
    static IRawParDoPtr MakeIntrusive()
    {
        return ::MakeIntrusive<TRawIdComputation>();
    }

private:
    TRowVtable RowVtable_;
    IRawOutputPtr Output_;

    Y_SAVELOAD_DEFINE_OVERRIDE(RowVtable_);
};

IRawParDoPtr MakeRawIdComputation(TRowVtable rowVtable)
{
    return ::MakeIntrusive<TRawIdComputation>(std::move(rowVtable));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren::NPrivate
