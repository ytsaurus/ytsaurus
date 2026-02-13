#include "misc.h"

#include <library/cpp/yt/error/error.h>

#include <yt/yt/core/actions/future.h>

namespace NRoren::NPrivate {

////////////////////////////////////////////////////////////////////////////////

NYT::TError MakeErrorFromCurrentException()
{
    try {
        throw;
    } catch (const NYT::TErrorException& e) {
        return e;
    } catch (const std::exception& e) {
        return e;
    } catch (const NYT::NConcurrency::TFiberCanceledException& e) {
        return NYT::TError(NYT::EErrorCode::Canceled, "Fiber canceled");
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren::NPrivate
