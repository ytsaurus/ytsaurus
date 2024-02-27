
#include <library/cpp/yt/logging/logger.h>
#include <yt/systest/reduce_dataset.h>
#include <yt/systest/util.h>

namespace NYT::NTest {

using namespace NNodeCmp;

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
        TCallState callState;
        std::vector<TNode> start(Inner_->Values().begin(), Inner_->Values().end());
        std::vector<TNode> prefix;

        for (int index : Dataset_->ReduceByIndices_) {
            prefix.push_back(start[index]);
        }
        Dataset_->Operation_.Reducer->StartRange(&callState, prefix);

        YT_VERIFY(Dataset_->ReduceByEqual(start, Inner_->Values()));

        while (!Inner_->Done() && Dataset_->ReduceByEqual(start, Inner_->Values())) {
            Dataset_->Operation_.Reducer->ProcessRow(&callState,
                ExtractInputValues(Inner_->Values(), Dataset_->Operation_.Reducer->InputColumns()));
            Inner_->Next();
        }

        auto result = Dataset_->Operation_.Reducer->FinishRange(&callState);

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
    Table_ = CreateTableFromReduceOperation(inner.table_schema(), Operation_, &ReduceByIndices_);
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

std::unique_ptr<IDatasetIterator> TReduceDataset::NewIterator() const
{
    return std::make_unique<TReduceDatasetIterator>(this);
}

}  // namespace NYT::NTest
