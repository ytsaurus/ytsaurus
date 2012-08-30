#pragma once

#include "common.h"

#include <ytlib/misc/error.h>
#include <ytlib/misc/pattern_formatter.h>

namespace NYT {
namespace NLog {

////////////////////////////////////////////////////////////////////////////////

Stroka FormatMessage(const Stroka& message);
Stroka FormatEvent(const TLogEvent& event, Stroka pattern);
TError ValidatePattern(const Stroka& pattern);
Stroka FormatDateTime(TInstant dateTime);
Stroka FormatLevel(ELogLevel level);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
} // namespace NLog
