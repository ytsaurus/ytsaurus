#include "event_timestamp_assigner.h"

#include <yt/yt/flow/library/cpp/common/message.h>
#include <yt/yt/flow/library/cpp/common/spec.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

class TEventTimestampAssigner
    : public IEventTimestampAssigner
{
public:
    explicit TEventTimestampAssigner(TEventTimestampAssignerSpecPtr spec)
        : Spec_(std::move(spec))
    { }

    void Assign(TMessage& message) const override
    {
        if (!Spec_ || !Spec_->Column) {
            return;
        }

        switch (Spec_->Format) {
            case ETimestampFormat::Seconds:
                message.EventTimestamp = GetColumnValue<TSystemTimestamp>(message, *Spec_->Column);
                break;
            case ETimestampFormat::MilliSeconds:
                message.EventTimestamp = TSystemTimestamp(GetColumnValue<ui64>(message, *Spec_->Column) / 1000);
                break;
            case ETimestampFormat::Iso8601:
                message.EventTimestamp = TSystemTimestamp(TInstant::ParseIso8601(GetColumnValue<std::string>(message, *Spec_->Column)).Seconds());
                break;
        }
        if (Spec_->LimitBySystemTimestamp && message.EventTimestamp > message.SystemTimestamp) {
            YT_VERIFY(message.SystemTimestamp != ZeroSystemTimestamp);
            message.EventTimestamp = message.SystemTimestamp;
        }
    }

private:
    const TEventTimestampAssignerSpecPtr Spec_;
};

////////////////////////////////////////////////////////////////////////////////

IEventTimestampAssignerPtr CreateEventTimestampAssigner(
    TEventTimestampAssignerSpecPtr spec)
{
    return New<TEventTimestampAssigner>(std::move(spec));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
