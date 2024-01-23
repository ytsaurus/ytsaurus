
#include <yt/systest/map_dataset.h>
#include <yt/systest/operation.h>
#include <yt/systest/util.h>

namespace NYT::NTest {

class TMapDatasetIterator : public IDatasetIterator
{
public:
    TMapDatasetIterator(const IMultiMapper& operation, std::unique_ptr<IDatasetIterator> innerIterator);

    TRange<TNode> Values() const override;
    bool Done() const override;
    void Next() override;

private:
    void FetchInner();

    const IMultiMapper& Operation_;
    std::unique_ptr<IDatasetIterator> Inner_;
    std::vector<std::vector<TNode>> Rows_;
    int RowsIndex_;
};

////////////////////////////////////////////////////////////////////////////////

TMapDatasetIterator::TMapDatasetIterator(const IMultiMapper& operation, std::unique_ptr<IDatasetIterator> innerIterator)
    : Operation_(operation)
    , Inner_(std::move(innerIterator))
    , RowsIndex_(0)
{
    FetchInner();
}

void TMapDatasetIterator::FetchInner()
{
    while (!Inner_->Done() && RowsIndex_ == std::ssize(Rows_)) {
        TCallState state;
        std::vector<TNode> input;
        input.reserve(Operation_.InputColumns().size());
        for (int index : Operation_.InputColumns()) {
            input.push_back(Inner_->Values()[index]);
        }
        Rows_ = Operation_.Run(&state, ExtractInputValues(Inner_->Values(), Operation_.InputColumns()));
        Inner_->Next();
        RowsIndex_ = 0;
    }
}

TRange<TNode> TMapDatasetIterator::Values() const
{
    return Rows_[RowsIndex_];
}

bool TMapDatasetIterator::Done() const
{
    return Inner_->Done() && RowsIndex_ == std::ssize(Rows_);
}

void TMapDatasetIterator::Next()
{
    ++RowsIndex_;
    FetchInner();
}

////////////////////////////////////////////////////////////////////////////////

TMapDataset::TMapDataset(const IDataset& inner, const IMultiMapper& operation)
    : Inner_(inner)
    , Operation_(operation)
    , Table_(CreateTableFromMapOperation(operation))
{
    for (const auto& dataColumn : Operation_.OutputColumns()) {
        ColumnStrings_.push_back(dataColumn.Name);
    }
}

////////////////////////////////////////////////////////////////////////////////

const TTable& TMapDataset::table_schema() const
{
    return Table_;
}

std::unique_ptr<IDatasetIterator> TMapDataset::NewIterator() const
{
    return make_unique<TMapDatasetIterator>(Operation_, Inner_.NewIterator());
}

}  // namespace NYT::NTest
