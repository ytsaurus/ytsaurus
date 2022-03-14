#pragma once

#include <yt/yt/core/misc/public.h>

#include <library/cpp/yt/logging/public.h>

namespace NYT::NLogging {

////////////////////////////////////////////////////////////////////////////////

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
DECLARE_REFCOUNTED_STRUCT(ILogWriter)
DECLARE_REFCOUNTED_STRUCT(IStreamLogOutput)
DECLARE_REFCOUNTED_STRUCT(ILogCompressionCodec)

class TLogManager;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLogging
