#pragma once

#include "public.h"
#include "job.h"

#include <yt/server/cell_node/public.h>

#include <yt/ytlib/table_client/public.h>

#include <yt/ytlib/api/public.h>

#include <yt/core/misc/nullable.h>

#include <yt/core/yson/string.h>

namespace NYT {
namespace NJobAgent {

////////////////////////////////////////////////////////////////////////////////

struct TJobTableDescriptor
{
    TJobTableDescriptor();

    struct TIndex
    {
        explicit TIndex(const NTableClient::TNameTablePtr& n);

        const int OperationIdHi;
        const int OperationIdLo;
        const int JobIdHi;
        const int JobIdLo;
        const int Type;
        const int State;
        const int StartTime;
        const int FinishTime;
        const int Address;
        const int Error;
        const int Spec;
        const int SpecVersion;
        const int Statistics;
        const int Events;
    };

    const NTableClient::TNameTablePtr NameTable;
    const TIndex Ids;
};

struct TJobSpecTableDescriptor
{
    TJobSpecTableDescriptor();

    struct TIndex
    {
        explicit TIndex(const NTableClient::TNameTablePtr& n);

        const int JobIdHi;
        const int JobIdLo;
        const int Spec;
        const int SpecVersion;
    };

    const NTableClient::TNameTablePtr NameTable;
    const TIndex Ids;
};

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
    void SetSpecEnabled(bool enable);

private:
    class TImpl;
    const TIntrusivePtr<TImpl> Impl_;
};

DEFINE_REFCOUNTED_TYPE(TStatisticsReporter)

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobAgent
} // namespace NYT
