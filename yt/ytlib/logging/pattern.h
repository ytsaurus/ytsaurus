#pragma once

#include "common.h"
#include <ytlib/misc/pattern_formatter.h>

namespace NYT {
namespace NLog {

////////////////////////////////////////////////////////////////////////////////

Stroka FormatMessage(const Stroka& message);
Stroka FormatEvent(const TLogEvent& event, Stroka pattern);
bool ValidatePattern(Stroka pattern, Stroka* errorMessage);
Stroka FormatDateTime(TInstant dateTime);
Stroka FormatLevel(ELogLevel level);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
} // namespace NLog
