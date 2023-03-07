#pragma once

#include <yt/core/misc/public.h>

namespace NYT::NLogTailer {

////////////////////////////////////////////////////////////////////////////////

class TBootstrap;

DECLARE_REFCOUNTED_CLASS(TLogRotationConfig)
DECLARE_REFCOUNTED_CLASS(TLogFileConfig)
DECLARE_REFCOUNTED_CLASS(TLogTableConfig)
DECLARE_REFCOUNTED_CLASS(TLogWriterLivenessCheckerConfig)
DECLARE_REFCOUNTED_CLASS(TLogTailerConfig)
DECLARE_REFCOUNTED_CLASS(TLogTailerBootstrapConfig)

DECLARE_REFCOUNTED_CLASS(TLogRotator)
DECLARE_REFCOUNTED_CLASS(TLogFileReader)
DECLARE_REFCOUNTED_CLASS(TLogTailer)
DECLARE_REFCOUNTED_CLASS(TLogWriterLivenessChecker)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLogTailer
