#ifndef HELPERS_INL_H_
#error "Direct inclusion of this file is not allowed, include helpers.h"
// For the sake of sane code completion.
#include "helpers.h"
#endif

namespace NYT::NAlertManager {

////////////////////////////////////////////////////////////////////////////////

template <class EErrorCode>
TAlert CreateAlert(const TError& error)
{
    auto category = static_cast<EErrorCode>(static_cast<int>(error.GetCode()));

    static const auto possibleAlertErrorCodes = TEnumTraits<EErrorCode>::GetDomainValues();
    YT_VERIFY(std::find(possibleAlertErrorCodes.begin(), possibleAlertErrorCodes.end(), category) != possibleAlertErrorCodes.end());

    return TAlert{FormatEnum(category), error};
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NAlertManager
