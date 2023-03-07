#pragma once

#include <yt/core/misc/public.h>

namespace NYT::NLogging {

////////////////////////////////////////////////////////////////////////////////

// Any change to this enum must be also propagated to FormatLevel.
DEFINE_ENUM(ELogLevel,
    (Minimum)
    (Trace)
    (Debug)
    (Info)
    (Warning)
    (Error)
    (Alert)
    (Fatal)
    (Maximum)
);

DEFINE_ENUM(ELogMessageFormat,
    (PlainText)
    (Structured)
);

DEFINE_ENUM(EWriterType,
    (File)
    (Stdout)
    (Stderr)
);

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TLogManagerConfig)
DECLARE_REFCOUNTED_CLASS(TFormatterConfig)
DECLARE_REFCOUNTED_CLASS(TWriterConfig)
DECLARE_REFCOUNTED_CLASS(TRuleConfig)

struct TLoggingCategory;
struct TLoggingPosition;
struct TLogEvent;
class TLogger;
class TLogManager;
class TRandomAccessGZipFile;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLogging
