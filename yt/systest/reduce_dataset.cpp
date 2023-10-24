
#include <library/cpp/yt/logging/logger.h>
#include <yt/systest/reduce_dataset.h>
#include <yt/systest/util.h>

namespace NYT::NTest {

using namespace NNodeCmp;

static std::vector<int> computeIndices(
    const std::vector<TString>& columns,
    const std::vector<TDataColumn>& tableColumns,
    const std::vector<int>& order)
{
    std::vector<int> result;
    for (const TString& columnName : columns) {
        auto position = std::lower_bound(order.begin(), order.end(), nullptr, [&](int pos, nullptr_t) {
            return tableColumns[pos].Name < columnName;
        });
        if (position == order.end() || tableColumns[*position].Name != columnName) {
            THROW_ERROR_EXCEPTION("Unknown column %v", columnName);
        }
        result.push_back(*position);
    }
    return result;
}

class TReduceDatasetIterator : public IDatasetIterator
{
public:
    explicit TReduceDatasetIterator(const TReduceDataset* dataset);

    TRange<TNode> Values() const override;
    bool Done() const override;
    void Next() override;

private:
    void ReduceRange();

    const TReduceDataset* Dataset_;
    std::unique_ptr<IDatasetIterator> Inner_;
    std::vector<std::vector<TNode>> Result_;
    int ResultIndex_;
};

////////////////////////////////////////////////////////////////////////////////

TReduceDatasetIterator::TReduceDatasetIterator(const TReduceDataset* dataset)
    : Dataset_(dataset),
      Inner_(dataset->Inner_.NewIterator())
{
    ReduceRange();
}

void TReduceDatasetIterator::ReduceRange()
{
    Result_.clear();
    ResultIndex_ = 0;
    while (Result_.empty() && !Inner_->Done()) {
        std::vector<std::vector<TNode>> innerRange;
        while (innerRange.empty() && !Inner_->Done()) {
            innerRange.push_back(std::vector<TNode>(Inner_->Values().begin(), Inner_->Values().end()));
            Inner_->Next();
            while (!Inner_->Done() && Dataset_->ReduceByEqual(innerRange.back(), Inner_->Values())) {
                innerRange.push_back(std::vector<TNode>(Inner_->Values().begin(), Inner_->Values().end()));
                Inner_->Next();
            }
        }

        if (!innerRange.empty()) {
            TCallState callState;

            std::vector<TNode> prefix;
            for (int index : Dataset_->ReduceByIndices_) {
                prefix.push_back(innerRange[0][index]);
            }

            std::vector<std::vector<TNode>> values;
            std::vector<TRange<TNode>> argument;
            for (const auto& entry : innerRange) {
                values.push_back(ExtractInputValues(entry, Dataset_->Operation_.Reducer->InputColumns()));
            }
            for (const auto& value : values) {
                argument.push_back(value);
            }

            auto result = Dataset_->Operation_.Reducer->Run(&callState, argument);

            Result_.clear();
            Result_.reserve(result.size());
            for (auto& entry : result) {
                std::vector<TNode> item(prefix);
                std::copy(entry.begin(), entry.end(), std::back_inserter(item));
                Result_.push_back(item);
            }
            ResultIndex_ = 0;
        }
    }
}

bool TReduceDatasetIterator::Done() const
{
    return Result_.empty() && Inner_->Done();
}

TRange<TNode> TReduceDatasetIterator::Values() const
{
    return Result_[ResultIndex_];
}

void TReduceDatasetIterator::Next()
{
    ++ResultIndex_;
    if (ResultIndex_ == std::ssize(Result_)) {
        ReduceRange();
    }
}

////////////////////////////////////////////////////////////////////////////////

TReduceDataset::TReduceDataset(const IDataset& inner, const TReduceOperation& operation)
    : Inner_(inner)
    , Operation_(operation)
{
    ComputeColumnIndices();
    for (int index : ReduceByIndices_) {
        Table_.DataColumns.push_back(Inner_.table_schema().DataColumns[index]);
    }
    std::copy(Operation_.Reducer->OutputColumns().begin(), Operation_.Reducer->OutputColumns().end(),
        std::back_inserter(Table_.DataColumns));
    Table_.SortColumns = std::ssize(ReduceByIndices_);
}

const TTable& TReduceDataset::table_schema() const
{
    return Table_;
}

bool TReduceDataset::ReduceByEqual(const std::vector<TNode>& lhs, TRange<TNode> rhs) const
{
    for (int i = 0; i < std::ssize(ReduceByIndices_); ++i) {
        int index = ReduceByIndices_[i];
        if (lhs[index] != rhs[index]) {
            return false;
        }
    }
    return true;
}

void TReduceDataset::ComputeColumnIndices()
{
    std::vector<int> order;
    const auto& columns = Inner_.table_schema().DataColumns;
    for (int i = 0; i < std::ssize(columns); ++i) {
        order.push_back(i);
    }
    std::sort(order.begin(), order.end(), [&columns](int lhs, int rhs) {
        return columns[lhs].Name < columns[rhs].Name;
    });

    ReduceByIndices_ = computeIndices(Operation_.ReduceBy, columns, order);

    for (int index : ReduceByIndices_) {
        if (index >= Inner_.table_schema().SortColumns) {
            THROW_ERROR_EXCEPTION("Table must be sorted by a reduce column");
        }
    }
}

std::unique_ptr<IDatasetIterator> TReduceDataset::NewIterator() const
{
    return std::make_unique<TReduceDatasetIterator>(this);
}

}  // namespace NYT::NTest
