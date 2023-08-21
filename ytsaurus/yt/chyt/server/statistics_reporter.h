#pragma once

#include "private.h"

#include <yt/yt/ytlib/api/native/public.h>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

struct IQueryStatisticsReporter
    : public TRefCounted
{
    virtual void ReportQueryStatistics(const TQueryContextPtr& queryContext) = 0;
};

DEFINE_REFCOUNTED_TYPE(IQueryStatisticsReporter)

IQueryStatisticsReporterPtr CreateQueryStatisticsReporter(
    TQueryStatisticsReporterConfigPtr config,
    NApi::NNative::IClientPtr client);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
