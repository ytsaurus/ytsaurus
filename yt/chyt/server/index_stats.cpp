#include "index_stats.h"

#include <IO/Operators.h>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

class TChytIndexStatBase
    : public IChytIndexStat
{
public:
    void DescribeIndex(DB::IQueryPlanStep::FormatSettings& formatSettings) const override
    {
        auto& out = formatSettings.out;
        out << "Type: " << GetType() << "\n";
    }

    std::unique_ptr<DB::JSONBuilder::JSONMap> DescribeIndex() const override
    {
        auto indexMap = std::make_unique<DB::JSONBuilder::JSONMap>();
        indexMap->add("Type", GetType());
        return indexMap;
    }
};

class TVirtualColumnIndexStat
    : public TChytIndexStatBase
{
public:
    TVirtualColumnIndexStat(int discardedTableCount, int inputTableCount)
        : DiscardedTableCount_(discardedTableCount)
        , InputTableCount_(inputTableCount)
    { }

    void DescribeIndex(DB::IQueryPlanStep::FormatSettings& formatSettings) const override
    {
        TChytIndexStatBase::DescribeIndex(formatSettings);
        auto &out = formatSettings.out;

        out << "Filtered tables: " << DiscardedTableCount_ << "/" << InputTableCount_;
    }

    std::unique_ptr<DB::JSONBuilder::JSONMap> DescribeIndex() const override
    {
        auto indexMap = TChytIndexStatBase::DescribeIndex();
        indexMap->add("Discarded tables", DiscardedTableCount_);
        indexMap->add("Input tables", InputTableCount_);
        return indexMap;
    }

    std::string GetType() const override
    {
        return "VirtualColumnIndex";
    }

private:
    const int DiscardedTableCount_;
    const int InputTableCount_;
};

class TKeyConditionIndexStat
    : public TChytIndexStatBase
{
public:
    TKeyConditionIndexStat(i64 rowCount, i64 dataWeight, i64 filteredRowCount, i64 filteredDataWeight)
        : RowCount_(rowCount)
        , DataWeight_(dataWeight)
        , FilteredRowCount_(filteredRowCount)
        , FilteredDataWeight_(filteredDataWeight)
    { }

    void DescribeIndex(DB::IQueryPlanStep::FormatSettings& formatSettings) const override
    {
        TChytIndexStatBase::DescribeIndex(formatSettings);
        auto &out = formatSettings.out;

        out << "Row count: " << RowCount_ << "\n";
        out << "Data weight: " << DataWeight_ << "\n";
        out << "Filtered row count: " << FilteredRowCount_ << "\n";
        out << "Filtered data weight: " << FilteredDataWeight_ << "\n";
    }

    std::unique_ptr<DB::JSONBuilder::JSONMap> DescribeIndex() const override
    {
        auto indexMap = TChytIndexStatBase::DescribeIndex();
        indexMap->add("Row count", RowCount_);
        indexMap->add("Data weight", DataWeight_);
        indexMap->add("Filtered row count", FilteredRowCount_);
        indexMap->add("Filtered data weight", FilteredDataWeight_);
        return indexMap;
    }

    std::string GetType() const override
    {
        return "KeyCondition";
    }

private:
    const i64 RowCount_;
    const i64 DataWeight_;
    const i64 FilteredRowCount_;
    const i64 FilteredDataWeight_;
};

std::shared_ptr<IChytIndexStat> CreateVirtualColumnIndexStat(int discardedTableCount, int inputTablesCount)
{
    return std::make_shared<TVirtualColumnIndexStat>(discardedTableCount, inputTablesCount);
}

std::shared_ptr<IChytIndexStat> CreateKeyConditionIndexStat(i64 rowCount, i64 dataWeight, i64 filteredRowCount, i64 filteredDataWeight)
{
    return std::make_shared<TKeyConditionIndexStat>(rowCount, dataWeight, filteredRowCount, filteredDataWeight);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
