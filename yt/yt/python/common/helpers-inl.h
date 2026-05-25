#ifndef HELPERS_INL_H_
#error "Direct inclusion of this file is not allowed, include helpers.h"
// For the sake of sane code completion.
#include "helpers.h"
#endif

namespace NYT::NPython {

////////////////////////////////////////////////////////////////////////////////

template <CFuture TFuture>
TErrorOr<typename TFuture::TValueType> SignalFriendlyWaitFor(TFuture future)
{
    while (true) {
        constexpr auto SignalCheckQuantum = TDuration::MilliSeconds(100);
        if (future.BlockingWait(SignalCheckQuantum)) {
            return future.GetOrCrash();
        }

        {
            TGilGuard gilGuard;
            auto signals = PyErr_CheckSignals();
            if (signals == -1) {
                auto error = TError(NYT::EErrorCode::Canceled, "Python signal received");
                future.Cancel(error);
                return error;
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NPython
