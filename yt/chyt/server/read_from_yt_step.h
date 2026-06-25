#pragma once

#include "private.h"

#include "table.h"
#include "index_stats.h"
#include "remote_source.h"

#include <yt/yt/core/misc/statistics.h>
#include <yt/yt/core/misc/statistic_path.h>

#include <Processors/QueryPlan/SourceStepWithFilter.h>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

class TReadFromYTStep
    : public DB::SourceStepWithFilterBase
{
public:
    TReadFromYTStep(
        const DB::SelectQueryInfo& queryInfo,
        TDistributedQueryExecutor executor,
        const std::vector<TTablePtr>& tables);

    String getName() const override;

    void initializePipeline(DB::QueryPipelineBuilder& pipeline, const DB::BuildQueryPipelineSettings&) override;

    void describeIndexes(DB::IQueryPlanStep::FormatSettings& formatSettings) const override;

    void describeIndexes(DB::JSONBuilder::JSONMap& map) const override;

    void describeActions(FormatSettings& formatSettings) const override;

    void describeActions(DB::JSONBuilder::JSONMap& map) const override;

private:
    const DB::SelectQueryInfo QueryInfo_;
    const std::vector<std::shared_ptr<IChytIndexStat>> IndexStats_;
    const DB::PrewhereInfoPtr PrewhereInfo_;
    TDistributedQueryExecutor Executor_;

    DB::ASTPtr DescribeFilterPushDown() const;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
