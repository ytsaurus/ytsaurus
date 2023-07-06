
#include <yt/systest/table_dataset.h>
#include <yt/systest/util.h>

namespace NYT::NTest {

class TTableDatasetIterator : public IDatasetIterator
{
public:
    TTableDatasetIterator(const TTable& table, TTableReaderPtr<TNode> reader);

    TRange<TNode> Values() const override;
    bool Done() const override;
    void Next() override;

private:
    const TTable& Table_;
    TTableReaderPtr<TNode> Reader_;
    const TColumnIndex ColumnIndex_;

    std::vector<TNode> Entry_;

    void Fetch();
};

////////////////////////////////////////////////////////////////////////////////

TTableDatasetIterator::TTableDatasetIterator(const TTable& table, TTableReaderPtr<TNode> reader)
    : Table_(table)
    , Reader_(reader)
    , ColumnIndex_(BuildColumnIndex(Table_.DataColumns))
{
    if (!Done()) {
        Fetch();
    }
}

void TTableDatasetIterator::Fetch()
{
    const auto& entry = Reader_->GetRow();

    auto mapRow = entry.AsMap();
    Entry_ = ArrangeValuesToIndex(ColumnIndex_, mapRow);
}

TRange<TNode> TTableDatasetIterator::Values() const
{
    return Entry_;
}

bool TTableDatasetIterator::Done() const
{
    return !Reader_->IsValid();
}

void TTableDatasetIterator::Next()
{
    Reader_->Next();
    if (!Done()) {
        Fetch();
    }
}

////////////////////////////////////////////////////////////////////////////////

TTableDataset::TTableDataset(const TTable& table, IClientPtr client, const TString& path)
    : Table_(table)
    , Client_(client)
    , Path_(path)
{
}

const TTable& TTableDataset::table_schema() const
{
    return Table_;
}

std::unique_ptr<IDatasetIterator> TTableDataset::NewIterator() const
{
    return std::make_unique<TTableDatasetIterator>(Table_, Client_->CreateTableReader<TNode>(Path_));
}

}  // namespace NYT::NTest
