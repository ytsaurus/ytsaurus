#ifndef HELPERS_INL_H_
#error "Direct inclusion of this file is not allowed, include helpers.h"
// For the sake of sane code completion.
#include "helpers.h"
#endif

namespace NYT::NAlertManager {

////////////////////////////////////////////////////////////////////////////////

template <class EErrorCode>
    requires std::is_enum_v<EErrorCode>
TAlert CreateAlert(EErrorCode errorCode, TString description, NProfiling::TTagList tags, TError error)
{
    return {errorCode, FormatEnum(errorCode), std::move(description), std::move(tags), std::move(error)};
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NAlertManager
