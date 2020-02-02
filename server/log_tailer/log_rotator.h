#pragma once

#include "config.h"
#include "public.h"

#include <yt/core/concurrency/periodic_executor.h>

namespace NYT::NLogTailer {

////////////////////////////////////////////////////////////////////////////////

class TLogRotator
    : public TRefCounted
{
public:
    TLogRotator(const TLogRotationConfigPtr& config, TBootstrap* bootstrap);

    void RotateLogs();

private:

    static TString GetLogSegmentPath(const TString& logFilePath, int segmentId);

    TBootstrap* const Bootstrap_;
    const TLogRotationConfigPtr Config_;

    NConcurrency::TPeriodicExecutorPtr LogRotatorExecutor_;
    std::vector<TString> LogFilePaths_;

    int RotationCount_ = 0;

    TInstant LastLogRotationTime_;
};

DEFINE_REFCOUNTED_TYPE(TLogRotator)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLogTailer
