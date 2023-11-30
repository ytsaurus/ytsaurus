#pragma once

#include "alert_manager.h"

namespace NYT::NAlertManager {

////////////////////////////////////////////////////////////////////////////////

template <class EErrorCode>
TAlert CreateAlert(const TError& error);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NAlertManager

#define HELPERS_INL_H_
#include "helpers-inl.h"
#undef HELPERS_INL_H_
