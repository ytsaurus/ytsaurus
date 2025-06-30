#pragma once

#include "private.h"

#include <Common/JSONBuilder.h>
#include <Core/Settings.h>
#include <IO/WriteBuffer.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

struct IChytIndexStat
{
    virtual void DescribeIndex(DB::IQueryPlanStep::FormatSettings& formatSettings) const = 0;

    virtual std::unique_ptr<DB::JSONBuilder::JSONMap> DescribeIndex() const = 0;

    virtual std::string GetType() const = 0;

    virtual ~IChytIndexStat() = default;
};

std::shared_ptr<IChytIndexStat> CreateVirtualColumnIndexStat(int discardedTableCount, int inputTablesCount);

std::shared_ptr<IChytIndexStat> CreateKeyConditionIndexStat(i64 rowCount, i64 dataWeight, i64 filteredRowCount, i64 filteredDataWeight);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
