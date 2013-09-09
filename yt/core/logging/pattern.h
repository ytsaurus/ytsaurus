#pragma once

#include "common.h"

#include <core/misc/error.h>
#include <core/misc/pattern_formatter.h>
#include <core/misc/raw_formatter.h>

namespace NYT {
namespace NLog {

////////////////////////////////////////////////////////////////////////////////

const int MessageBufferSize = 65556;
typedef TRawFormatter<MessageBufferSize> TMessageBuffer;

void FormatMessage(TMessageBuffer* out, const Stroka& message);
void FormatDateTime(TMessageBuffer* out, TInstant dateTime);
void FormatLevel(TMessageBuffer* out, ELogLevel level);

TError ValidatePattern(const Stroka& pattern);
Stroka FormatEvent(const TLogEvent& event, const Stroka& pattern);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
} // namespace NLog
