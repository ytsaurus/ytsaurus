#include "throttle.h"

#include "bigrt_execution_context.h"

namespace NRoren::NPrivate {

////////////////////////////////////////////////////////////////////////////////

class TReportThrottlerProcessedParDo : public NRoren::IDoFn<ui64, void>
{
public:
    TReportThrottlerProcessedParDo() = default;

    explicit TReportThrottlerProcessedParDo(ui64 flushSize)
        : FlushSize_(flushSize)
    {
    }

    void Start(NRoren::TOutput<void>&) override
    {
        Sum_ = 0;
    }

    void Do(const ui64& input, NRoren::TOutput<void>& output) override
    {
        Sum_ += input;
        if (Sum_ >= FlushSize_) {
            Finish(output);
        }
    }

    void Finish(NRoren::TOutput<void>&) override
    {
        if (GetExecutionContext()->GetExecutorName() == "bigrt") {
            auto ctx = GetExecutionContext()->As<IBigRtExecutionContext>();
            NPrivate::TBigRtExecutionContextOps::ThrottlerReportProcessed(ctx, Sum_);
        }
        Sum_ = 0;
    }

    Y_SAVELOAD_DEFINE_OVERRIDE(FlushSize_);

private:
    ui64 FlushSize_ = 0;
    ui64 Sum_ = 0;
};

////////////////////////////////////////////////////////////////////////////////

NRoren::TTransform<ui64, void> ReportThrottlerProcessedImpl(ui64 flushSize)
{
    return NRoren::MakeParDo<TReportThrottlerProcessedParDo>(flushSize);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren::NPrivate
