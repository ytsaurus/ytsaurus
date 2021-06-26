#pragma once

#include <yt/yt/core/misc/public.h>

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

DEFINE_ENUM(ELogFamily,
    (PlainText)
    (Structured)
);

DEFINE_ENUM(ELogFormat,
    (PlainText)
    (Json)
    // Legacy alias for JSON.
    (Structured)
    (Yson)
)

DEFINE_ENUM(EWriterType,
    (File)
    (Stdout)
    (Stderr)
);

DEFINE_ENUM(ECompressionMethod,
    (Gzip)
    (Zstd)
);

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TLogManagerConfig)
DECLARE_REFCOUNTED_CLASS(TLogManagerDynamicConfig)
DECLARE_REFCOUNTED_CLASS(TFormatterConfig)
DECLARE_REFCOUNTED_CLASS(TWriterConfig)
DECLARE_REFCOUNTED_CLASS(TRuleConfig)

struct TLoggingCategory;
struct TLoggingAnchor;
struct TLogEvent;
class TLogger;
class TLogManager;
class TRandomAccessGZipFile;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLogging
