#pragma once

#include "private.h"

#include "table.h"
#include "index_stats.h"

#include <yt/yt/core/misc/statistics.h>
#include <yt/yt/core/misc/statistic_path.h>

#include <Processors/QueryPlan/ReadFromPreparedSource.h>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

class TReadFromYTStep
    : public DB::ReadFromPreparedSource
{
public:
    TReadFromYTStep(
        DB::Pipe pipe,
        const DB::SelectQueryInfo& queryInfo,
        std::vector<std::shared_ptr<IChytIndexStat>> indexStats,
        const std::vector<TTablePtr>& tables);

    String getName() const override;

    void describeIndexes(DB::IQueryPlanStep::FormatSettings& formatSettings) const override;

    void describeIndexes(DB::JSONBuilder::JSONMap& map) const override;

    void describeActions(FormatSettings& formatSettings) const override;

    void describeActions(DB::JSONBuilder::JSONMap& map) const override;

private:
    const std::vector<std::shared_ptr<IChytIndexStat>> IndexStats_;
    const DB::PrewhereInfoPtr PrewhereInfo_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
