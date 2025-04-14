#include "raw_par_do.h"


namespace NRoren::NPrivate {

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

    void MoveDo(void* rows, int count) override
    {
        Output_->MoveRaw(rows, count);
    }

    void Finish() override
    {
        Output_ = nullptr;
    }

    [[nodiscard]] TDefaultFactoryFunc GetDefaultFactory() const override
    {
        return &TRawIdComputation::New;
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
    static IRawParDoPtr New()
    {
        return NYT::New<TRawIdComputation>();
    }

private:
    TRowVtable RowVtable_;
    IRawOutputPtr Output_;

    Y_SAVELOAD_DEFINE_OVERRIDE(RowVtable_);
};

IRawParDoPtr MakeRawIdComputation(TRowVtable rowVtable)
{
    return NYT::New<TRawIdComputation>(std::move(rowVtable));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren::NPrivate
