
#include <library/cpp/yt/logging/logger.h>
#include <library/cpp/yson/node/node.h>
#include <yt/systest/sort_dataset.h>

namespace NYT::NTest {

using namespace NNodeCmp;

class TSortIterator : public IDatasetIterator
{
public:
    TSortIterator(const TSortDataset::TDataEntry* data, i64 NumEntries);

    TRange<TNode> Values() const override;
    bool Done() const override;
    void Next() override;

private:
    const TSortDataset::TDataEntry* Data_;
    const TSortDataset::TDataEntry* End_;
};

////////////////////////////////////////////////////////////////////////////////

TSortIterator::TSortIterator(const TSortDataset::TDataEntry* data, i64 NumEntries)
    : Data_(data)
    , End_(Data_ + NumEntries)
{
}

TRange<TNode> TSortIterator::Values() const
{
    return TRange<TNode>(Data_->Values);
}

bool TSortIterator::Done() const
{
    return Data_ == End_;
}

void TSortIterator::Next()
{
    ++Data_;
}

////////////////////////////////////////////////////////////////////////////////

TSortDataset::TSortDataset(const IDataset& inner, const TSortOperation& operation)
    : Inner_(inner)
    , Operation_(operation)
    , NumColumns_(std::ssize(Inner_.table_schema().DataColumns))
    , ColumnSorted_(std::ssize(Inner_.table_schema().DataColumns), false)
{
    BuildOutputTableSchema();
    ConsumeAndSortInner();
}

void TSortDataset::BuildOutputTableSchema()
{
    std::unordered_map<TString, int> index;
    for (int i = 0; i < std::ssize(Inner_.table_schema().DataColumns); i++) {
        index.insert(std::make_pair(Inner_.table_schema().DataColumns[i].Name, i));
    }
    for (const auto& sortColumn : Operation_.SortBy) {
        auto position = index.find(sortColumn);
        if (position == index.end()) {
            THROW_ERROR_EXCEPTION("Sort column %v missing from input schema", sortColumn);
        }
        Table_.DataColumns.push_back(Inner_.table_schema().DataColumns[position->second]);
        ColumnSorted_[position->second] = true;
        SortByIndices_.push_back(position->second);
    }
    for (int i = 0; i < NumColumns_; i++) {
        if (ColumnSorted_[i]) {
            continue;
        }
        Table_.DataColumns.push_back(Inner_.table_schema().DataColumns[i]);
    }
    Table_.SortColumns = std::ssize(Operation_.SortBy);
}

std::vector<TNode> TSortDataset::BuildValues(TRange<TNode> values) const
{
    std::vector<TNode> result;
    for (int i = 0; i < std::ssize(SortByIndices_); i++) {
        result.push_back(values[SortByIndices_[i]]);
    }
    for (int i = 0; i < NumColumns_; i++) {
        if (ColumnSorted_[i]) {
            continue;
        }
        result.push_back(values[i]);
    }
    NYT::NLogging::TLogger Logger("test");
    return result;
}

void TSortDataset::ConsumeAndSortInner()
{
    for (auto iterator = Inner_.NewIterator(); !iterator->Done(); iterator->Next()) {
        Data_.push_back(TDataEntry{BuildValues(iterator->Values())});
    }

    std::sort(Data_.begin(), Data_.end(), [this](const TDataEntry& lhs, const TDataEntry& rhs) {
        return Comparator(lhs, rhs) < 0;
    });
}

int TSortDataset::Comparator(const TDataEntry& lhs, const TDataEntry& rhs) const
{
    for (int i = 0; i < Table_.SortColumns; ++i) {
        if (lhs.Values[i] != rhs.Values[i]) {
            if (lhs.Values[i] < rhs.Values[i]) {
                return -1;
            } else {
                return 1;
            }
        }
    }
    return 0;
}

const TTable& TSortDataset::table_schema() const
{
    return Table_;
}

std::unique_ptr<IDatasetIterator> TSortDataset::NewIterator() const
{
    return std::make_unique<TSortIterator>(&Data_[0], std::ssize(Data_));
}

}  // namespace NYT::NTest
