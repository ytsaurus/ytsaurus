#pragma once

#include "public.h"

#include <yt/core/misc/raw_formatter.h>

#include <util/generic/size_literals.h>

namespace NYT::NLogging {

////////////////////////////////////////////////////////////////////////////////

constexpr int MessageBufferSize = 64_KB;
constexpr int MessageBufferWatermarkSize = 256;
using TMessageBuffer = TRawFormatter<MessageBufferSize>;

void FormatMessage(TMessageBuffer* out, TStringBuf message);
void FormatDateTime(TMessageBuffer* out, TInstant dateTime);
void FormatLevel(TMessageBuffer* out, ELogLevel level);

////////////////////////////////////////////////////////////////////////////////

} // namespace NLogging::NYT
