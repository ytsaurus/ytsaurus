#pragma once

#include "private.h"

#include <yt/yt/client/table_client/schema.h>

#include <Columns/IColumn.h>
#include <Common/PODArray.h>
#include <Core/Block.h>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

struct TBlockWithFilter
{
    explicit TBlockWithFilter(i64 rowCount);

    DB::Block Block;
    i64 RowCount;
    DB::IColumn::Filter Filter;
    i64 RowCountAfterFilter;
};

struct TFilterInfo
{
    //! Filter actions to execute.
    DB::ExpressionActionsPtr Actions;
    //! The name of the column that is used as a filter.
    //! The column type is either `UInt8` or `Nullable(UInt8)`.
    std::string FilterColumnName;
    //! If |true|, the filter column is not needed for the following steps and can be removed.
    bool RemoveFilterColumn = false;

    //! Execute filter actions and merge the resulting filter columns with the passed one.
    //! NB: Modifies the passed TBlockWithFilter.
    void Execute(TBlockWithFilter& blockWithFilter) const;
};

////////////////////////////////////////////////////////////////////////////////

struct TReadStepWithFilter
{
    //! Additional columns to read and convert on the current step.
    std::vector<NTableClient::TColumnSchema> Columns;
    //! Filter to execute on current step.
    //! The filter actions may use columns from this and all previous steps.
    std::optional<TFilterInfo> FilterInfo;
};

////////////////////////////////////////////////////////////////////////////////


struct TReadPlanWithFilter final
{
    //! Read and filter steps that should be executed sequentially.
    std::vector<TReadStepWithFilter> Steps;
    //! If |true|, the storage should filter out rows according to filter infos.
    //! Otherwise, it's enough to just execute filter actions. The actual filter
    //! step would be performed later by CH.
    bool NeedFilter = false;

    int GetReadColumnCount() const;
    bool SuitableForTwoStagePrewhere() const;
};

DEFINE_REFCOUNTED_TYPE(TReadPlanWithFilter)

////////////////////////////////////////////////////////////////////////////////

TReadPlanWithFilterPtr BuildSimpleReadPlan(const std::vector<NTableClient::TColumnSchema>& columns);

TReadPlanWithFilterPtr BuildReadPlanWithPrewhere(
    const std::vector<NTableClient::TColumnSchema>& columns,
    const DB::PrewhereInfoPtr& prewhereInfo,
    const DB::Settings& settings);

TReadPlanWithFilterPtr ExtractPrewhereOnlyReadPlan(const TReadPlanWithFilterPtr& readPlan);

////////////////////////////////////////////////////////////////////////////////

DB::Block DeriveHeaderBlockFromReadPlan(const TReadPlanWithFilterPtr& readPlan,  const TCompositeSettingsPtr& settings);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
