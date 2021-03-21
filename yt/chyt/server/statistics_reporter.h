#pragma once

#include "private.h"

#include <yt/yt/ytlib/api/native/public.h>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

class TQueryStatisticsReporter
    : public TRefCounted
{
public:
    TQueryStatisticsReporter(
        TQueryStatisticsReporterConfigPtr config,
        const NApi::NNative::IClientPtr& client);

    ~TQueryStatisticsReporter();

    void ReportQueryStatistics(const TQueryContextPtr& queryContext);

private:
    class TImpl;
    const TIntrusivePtr<TImpl> Impl_;
};

DEFINE_REFCOUNTED_TYPE(TQueryStatisticsReporter)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
