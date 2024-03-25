#pragma once

#include "alert_manager.h"

namespace NYT::NAlertManager {

////////////////////////////////////////////////////////////////////////////////

template <class EErrorCode>
    requires std::is_enum_v<EErrorCode>
TAlert CreateAlert(EErrorCode errorCode, TString description, NProfiling::TTagList tags, TError error);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NAlertManager

#define HELPERS_INL_H_
#include "helpers-inl.h"
#undef HELPERS_INL_H_
