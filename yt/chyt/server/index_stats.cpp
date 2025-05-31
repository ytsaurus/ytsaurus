#include "index_stats.h"

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
    TVirtualColumnIndexStat(int discardedTableCount, int inputTablesCount)
        : DiscardedTableCount_(discardedTableCount)
        , InputTablesCount_(inputTablesCount)
    { }

    void DescribeIndex(DB::IQueryPlanStep::FormatSettings& formatSettings) const override
    {
        TChytIndexStatBase::DescribeIndex(formatSettings);
        auto &out = formatSettings.out;

        out << "Filtered tables: " << DiscardedTableCount_ << "/" << InputTablesCount_;
    }

    std::unique_ptr<DB::JSONBuilder::JSONMap> DescribeIndex() const override
    {
        auto indexMap = TChytIndexStatBase::DescribeIndex();
        indexMap->add("Discarded tables", DiscardedTableCount_);
        indexMap->add("Input tables", InputTablesCount_);
        return indexMap;
    }

    std::string GetType() const override
    {
        return "VirtualColumnIndex";
    }

private:
    int DiscardedTableCount_;
    int InputTablesCount_;
};

class TKeyConditionIndexStat
    : public TChytIndexStatBase
{
public:
    TKeyConditionIndexStat(int rowCount, int dataWeight, int filteredRowCount, int filteredDataWeight)
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
    int RowCount_;
    int DataWeight_;
    int FilteredRowCount_;
    int FilteredDataWeight_;
};

std::shared_ptr<IChytIndexStat> CreateVirtualColumnIndexStat(int discardedTableCount, int inputTablesCount) {
    return std::make_shared<TVirtualColumnIndexStat>(discardedTableCount, inputTablesCount);
}

std::shared_ptr<IChytIndexStat> CreateKeyConditionIndexStat(int rowCount, int dataWeight, int filteredRowCount, int filteredDataWeight) {
    return std::make_shared<TKeyConditionIndexStat>(rowCount, dataWeight, filteredRowCount, filteredDataWeight);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
