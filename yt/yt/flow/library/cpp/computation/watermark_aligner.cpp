#include "watermark_aligner.h"

#include <yt/yt/flow/library/cpp/common/flow_view.h>
#include <yt/yt/flow/library/cpp/common/spec.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

class TWatermarkAligner
    : public IWatermarkAligner
{
public:
    TWatermarkAligner(
        TWatermarkAlignmentSpecPtr spec,
        NLogging::TLogger logger)
        : Spec_(std::move(spec))
        , Logger(std::move(logger))
    { }

    bool IsAllowToRead(const TSystemTimestamp& localReadWatermark, TWatermarkStatePtr watermarkState) override
    {
        if (!Spec_) {
            return true;
        }
        auto watermark = watermarkState->GetAlignmentEventWatermark(Spec_->GroupName);
        auto driftBound = Spec_->DriftBound;
        if (TInstant::Seconds(watermark.Underlying()) + driftBound < TInstant::Seconds(localReadWatermark.Underlying())) {
            YT_LOG_DEBUG("Read too much ahead (AlignmentGroup: %v, LocalWatermark: %v, GroupWatermark: %v, DriftBound: %v)",
                Spec_->GroupName,
                localReadWatermark,
                watermark,
                driftBound);
            return false;
        }
        return true;
    }

private:
    const TWatermarkAlignmentSpecPtr Spec_;
    const NLogging::TLogger Logger;
};

////////////////////////////////////////////////////////////////////////////////

IWatermarkAlignerPtr CreateWatermarkAligner(
    TWatermarkAlignmentSpecPtr spec,
    NLogging::TLogger logger)
{
    return New<TWatermarkAligner>(std::move(spec), std::move(logger));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
