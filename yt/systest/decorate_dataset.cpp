
#include <yt/systest/decorate_dataset.h>

namespace NYT::NTest {

TDecorateDataset::TDecorateDataset(std::unique_ptr<IDataset> inner, std::vector<TString> deletedColumnNames)
    : Inner_(std::move(inner))
    , Table_(Inner_->table_schema())
{
    for (const auto& name : deletedColumnNames) {
        Table_.DeletedColumnNames.push_back(name);
    }
}

const TTable& TDecorateDataset::table_schema() const
{
    return Table_;
}

std::unique_ptr<IDatasetIterator> TDecorateDataset::NewIterator() const
{
    return Inner_->NewIterator();
}

}  // namespace NYT::NTest
