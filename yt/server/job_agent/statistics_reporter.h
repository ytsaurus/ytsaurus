#pragma once

#include "public.h"
#include "job.h"

#include <yt/server/cell_node/public.h>

#include <yt/ytlib/table_client/public.h>

#include <yt/client/api/public.h>
#include <yt/client/api/operation_archive_schema.h>

#include <yt/core/misc/optional.h>

#include <yt/core/yson/string.h>

namespace NYT {
namespace NJobAgent {

////////////////////////////////////////////////////////////////////////////////

using NYT::NApi::TJobTableDescriptor;
using NYT::NApi::TJobSpecTableDescriptor;

////////////////////////////////////////////////////////////////////////////////

//! Periodically reports job statistics to the dynamic table.
class TStatisticsReporter
    : public TRefCounted
{
public:
    TStatisticsReporter(
        TStatisticsReporterConfigPtr reporterConfig,
        NCellNode::TBootstrap* bootstrap);

    void ReportStatistics(TJobStatistics&& statistics);
    void SetEnabled(bool enable);
    void SetSpecEnabled(bool enable);
    void SetStderrEnabled(bool enable);
    void SetFailContextEnabled(bool enable);
    void SetOperationArchiveVersion(int version);
    int ExtractWriteFailuresCount();
    bool GetQueueIsTooLarge();

private:
    class TImpl;
    const TIntrusivePtr<TImpl> Impl_;
};

DEFINE_REFCOUNTED_TYPE(TStatisticsReporter)

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobAgent
} // namespace NYT
