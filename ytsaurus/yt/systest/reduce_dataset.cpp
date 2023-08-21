
#include <yt/systest/reduce_dataset.h>

namespace NYT::NTest {

using namespace NNodeCmp;

class TReduceDatasetIterator : public IDatasetIterator
{
public:
    TReduceDatasetIterator(const TReduceDataset* dataset);

    TRange<TNode> Values() const override;
    bool Done() const override;
    void Next() override;

private:
    void ReduceRange();

    const TReduceDataset* Dataset_;
    int Lo_, Hi_;

    std::vector<std::vector<TNode>> Result_;
    int ResultIndex_;
};

////////////////////////////////////////////////////////////////////////////////

TReduceDatasetIterator::TReduceDatasetIterator(const TReduceDataset* dataset)
    : Dataset_(dataset)
    , Lo_(0)
    , Hi_(0)
{
    ReduceRange();
}

void TReduceDatasetIterator::ReduceRange()
{
    while (Hi_ == Lo_ &&
        Lo_ < std::ssize(Dataset_->DataEntryIndex_) &&
        ResultIndex_ == std::ssize(Result_)) {
        while (Hi_ < std::ssize(Dataset_->DataEntryIndex_) &&
            Dataset_->SortColumnsComparator(Dataset_->DataEntryIndex_[Lo_], Dataset_->DataEntryIndex_[Hi_])) {
            ++Hi_;
        }

        TCallState callState;

        std::vector<TRange<TNode>> argument;
        for (int i = Lo_; i < Hi_; i++) {
            argument.push_back(Dataset_->Data_[Dataset_->DataEntryIndex_[i]].Values);
        }

        Lo_ = Hi_;

        Result_ = Dataset_->Operation_.Reducer->Run(&callState, argument);
        ResultIndex_ = 0;
    }
}

bool TReduceDatasetIterator::Done() const
{
    return Lo_ == std::ssize(Dataset_->DataEntryIndex_) && ResultIndex_ == std::ssize(Result_);
}

TRange<TNode> TReduceDatasetIterator::Values() const
{
    return Result_[ResultIndex_];
}

void TReduceDatasetIterator::Next()
{
    ++ResultIndex_;
    while (ResultIndex_ == std::ssize(Result_) && !Done()) {
        ReduceRange();
    }
}

////////////////////////////////////////////////////////////////////////////////

TReduceDataset::TReduceDataset(const IDataset& inner, const TReduceOperation& operation)
    : Inner_(inner)
    , Operation_(operation)
    , Table_{std::vector<TDataColumn>{
        Operation_.Reducer->OutputColumns().begin(), Operation_.Reducer->OutputColumns().end()}}
{
}

const TTable& TReduceDataset::table_schema() const
{
    return Table_;
}

int TReduceDataset::SortColumnsComparator(int lhs, int rhs) const
{
    const auto& dataLhs = Data_[lhs];
    const auto& dataRhs = Data_[rhs];
    for (int i = 0; i < std::ssize(SortColumnIndices_); ++i) {
        int index = SortColumnIndices_[i];
        if (dataLhs.Values[index] != dataRhs.Values[index]) {
            if (dataLhs.Values[index] < dataRhs.Values[index]) {
                return -1;
            } else {
                return 1;
            }
        }
    }
    return 0;
}

void TReduceDataset::ComputeSortColumnIndices()
{
    std::vector<int> order;
    const auto& columns = Inner_.table_schema().DataColumns;
    for (int i = 0; i < std::ssize(columns); ++i) {
        order.push_back(i);
    }
    std::sort(order.begin(), order.end(), [&columns](int lhs, int rhs) {
        return columns[lhs].Name < columns[rhs].Name;
    });

    for (const TString& columnName : Operation_.SortBy) {
        auto position = std::lower_bound(order.begin(), order.end(), nullptr, [&](int pos, nullptr_t) {
            return columns[pos].Name < columnName;
        });
        if (position == order.end() || columns[*position].Name != columnName) {
            break;
        }
        SortColumnIndices_.push_back(*position);
    }
}

void TReduceDataset::ConsumeAndSortInner()
{
    for (auto iterator = Inner_.NewIterator(); !iterator->Done(); iterator->Next()) {
        Data_.push_back(DataEntry{
                std::vector<TNode>(iterator->Values().begin(), iterator->Values().end())});
    }

    DataEntryIndex_.reserve(Data_.size());
    for (int i = 0; i < std::ssize(DataEntryIndex_); ++i) {
        DataEntryIndex_.push_back(i);
    }

    std::sort(DataEntryIndex_.begin(), DataEntryIndex_.end(), [this](int lhs, int rhs) {
        return SortColumnsComparator(lhs, rhs) < 0;
    });
}

std::unique_ptr<IDatasetIterator> TReduceDataset::NewIterator() const
{
    return nullptr;
}

}  // namespace NYT::NTest
