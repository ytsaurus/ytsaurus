#pragma once

#include "public.h"
#include "job.h"

#include <yt/server/cell_node/public.h>

#include <yt/ytlib/api/public.h>

#include <yt/core/misc/nullable.h>

#include <yt/core/yson/string.h>

namespace NYT {
namespace NJobAgent {

////////////////////////////////////////////////////////////////////////////////

//! Periodically reports job statistics to dynamic table.
class TStatisticsReporter
    : public TRefCounted
{
public:
    TStatisticsReporter(
        TStatisticsReporterConfigPtr reporterConfig,
        NCellNode::TBootstrap* bootstrap);

    void ReportStatistics(TJobStatistics&& statistics);
    void SetEnabled(bool enable);

private:
    class TImpl;
    const TIntrusivePtr<TImpl> Impl_;
};

DEFINE_REFCOUNTED_TYPE(TStatisticsReporter)

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobAgent
} // namespace NYT
