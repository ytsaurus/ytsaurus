
#include <yt/systest/bootstrap_dataset.h>

namespace NYT::NTest {

namespace {

class TBootstrapDatasetIterator : public IDatasetIterator
{
public:
    TBootstrapDatasetIterator(int count)
        : RowCount_(count)
        , Pos_(0)
    {
        Node_[0] = TNode(Pos_);
    }

    TRange<TNode> Values() const override {
        return Node_;
    }

    void Next() override {
        ++Pos_;
        Node_[0] = TNode(Pos_);
    }

    bool Done() const override {
        return Pos_ == RowCount_;
    }

private:
    int RowCount_;
    int Pos_;
    TNode Node_[1];
};

}  // namespace

const TTable& TBootstrapDataset::table_schema() const
{
    return BootstrapTable_;
}

std::unique_ptr<IDatasetIterator> TBootstrapDataset::NewIterator() const
{
    return std::make_unique<TBootstrapDatasetIterator>(RowCount_);
}

TBootstrapDataset::TBootstrapDataset(int RowCount)
    : RowCount_(RowCount)
{
    TDataColumn column{"key", NProto::EColumnType::EInt64};
    BootstrapTable_.DataColumns.push_back(column);
}

}  // namespace NYT::NTest
