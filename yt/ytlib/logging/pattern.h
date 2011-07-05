#pragma once

#include "common.h"
#include "../misc/pattern_formatter.h"

namespace NYT {
namespace NLog {

////////////////////////////////////////////////////////////////////////////////

Stroka FormatEvent(const TLogEvent& event, Stroka pattern);
bool ValidatePattern(Stroka pattern, Stroka* errorMessage);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
} // namespace NLog
