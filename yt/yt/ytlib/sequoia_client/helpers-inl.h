#ifndef HELPERS_INL_H_
#error "Direct inclusion of this file is not allowed, include helpers.h"
// For the sake of sane code completion.
#include "helpers.h"
#endif

namespace NYT::NSequoiaClient {

////////////////////////////////////////////////////////////////////////////////

template <class T>
TErrorOr<T> MaybeWrapSequoiaRetriableError(
    std::conditional_t<std::is_void_v<T>, const TError&, TErrorOr<T>&&> result)
{
    if (!result.IsOK() &&
        !result.FindMatching(EErrorCode::SequoiaRetriableError) &&
        IsRetriableSequoiaError(result))
    {
        return TError(EErrorCode::SequoiaRetriableError, "Retriable Sequoia error")
            << std::forward<decltype(result)>(result);
    }

    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSequoiaClient
