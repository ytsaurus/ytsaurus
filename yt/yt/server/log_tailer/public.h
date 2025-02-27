#pragma once

#include <yt/yt/core/misc/public.h>

namespace NYT::NLogTailer {

////////////////////////////////////////////////////////////////////////////////

class TBootstrap;

DECLARE_REFCOUNTED_STRUCT(TLogRotationConfig)
DECLARE_REFCOUNTED_STRUCT(TLogFileConfig)
DECLARE_REFCOUNTED_STRUCT(TLogTableConfig)
DECLARE_REFCOUNTED_STRUCT(TLogWriterLivenessCheckerConfig)
DECLARE_REFCOUNTED_STRUCT(TLogTailerConfig)
DECLARE_REFCOUNTED_STRUCT(TLogTailerBootstrapConfig)

DECLARE_REFCOUNTED_CLASS(TLogRotator)
DECLARE_REFCOUNTED_CLASS(TLogFileReader)
DECLARE_REFCOUNTED_CLASS(TLogTailer)
DECLARE_REFCOUNTED_CLASS(TLogWriterLivenessChecker)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLogTailer
