
#include <yt/systest/unittests/stub_dataset.h>

namespace NYT::NTest {

class TStubIterator : public IDatasetIterator
{
public:
    explicit TStubIterator(TRange<std::vector<TNode>> values);

    virtual TRange<TNode> Values() const override;
    virtual bool Done() const override;
    virtual void Next() override;

private:
    TRange<std::vector<TNode>> Values_;
    ssize_t Pos_;
};

////////////////////////////////////////////////////////////////////////////////

TStubIterator::TStubIterator(TRange<std::vector<TNode>> values)
    : Values_(values)
    , Pos_(0)
{
}

TRange<TNode> TStubIterator::Values() const
{
    return Values_[Pos_];
}

bool TStubIterator::Done() const
{
    return Pos_ == std::ssize(Values_);
}

void TStubIterator::Next()
{
    ++Pos_;
}

////////////////////////////////////////////////////////////////////////////////

TStubDataset::TStubDataset(const TTable& table, std::vector<std::vector<TNode>> data)
    : Table_(table)
    , Data_(std::move(data))
{
}

const TTable& TStubDataset::table_schema() const
{
    return Table_;
}

std::unique_ptr<IDatasetIterator> TStubDataset::NewIterator() const
{
    return std::make_unique<TStubIterator>(Data_);
}

}  // namespace NYT::NTest
