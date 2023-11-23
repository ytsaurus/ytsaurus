#pragma once

#include "alert_manager.h"

namespace NYT::NAlertManager {

////////////////////////////////////////////////////////////////////////////////

template <class EErrorCode>
TAlert CreateAlert(const TError& err)
{
    auto category = static_cast<EErrorCode>(static_cast<int>(err.GetCode()));

    static const auto possibleAlertErrorCodes = TEnumTraits<EErrorCode>::GetDomainValues();
    YT_VERIFY(std::find(possibleAlertErrorCodes.begin(), possibleAlertErrorCodes.end(), category) != possibleAlertErrorCodes.end());

    return TAlert{FormatEnum(category), err};
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NAlertManager
