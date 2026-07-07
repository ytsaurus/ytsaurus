#ifndef HELPERS_INL_H_
#error "Direct inclusion of this file is not allowed, include helpers.h"
// For the sake of sane code completion.
#include "helpers.h"
#endif

namespace NYT::NHydra {

////////////////////////////////////////////////////////////////////////////////

template <typename TFunc, typename... TArgs>
auto InvokeAndWrapHydraException(TFunc&& func, TArgs&&... args)
{
    try {
        return std::invoke(std::forward<TFunc>(func), std::forward<TArgs>(args)...);
    } catch (const TErrorException& error) {
        if (error.Error().GetCode() == EErrorCode::ExpectedMutationHandlerException) {
            throw;
        }

        THROW_ERROR_EXCEPTION(EErrorCode::ExpectedMutationHandlerException,
            "Error executing mutation")
            << error;

    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION(EErrorCode::ExpectedMutationHandlerException,
            "Error executing mutation")
            << ex;
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra
