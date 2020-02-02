#pragma once

#include "config.h"
#include "public.h"

#include <yt/core/concurrency/periodic_executor.h>

namespace NYT::NLogTailer {

////////////////////////////////////////////////////////////////////////////////

class TLogWriterLivenessChecker
    : public TRefCounted
{
public:
    TLogWriterLivenessChecker(
        const TLogWriterLivenessCheckerConfigPtr& config,
        TBootstrap* bootstrap);

    void CheckLiveness();
private:

    TBootstrap* const Bootstrap_;
    const TLogWriterLivenessCheckerConfigPtr Config_;

    TInstant LastLogWriterLivenessCheckTime_;
};

DEFINE_REFCOUNTED_TYPE(TLogWriterLivenessChecker)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLogTailer

