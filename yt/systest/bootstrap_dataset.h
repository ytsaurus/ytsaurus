#pragma once

#include <yt/systest/dataset.h>

namespace NYT::NTest {

class TBootstrapDataset : public IDataset
{
public:
    explicit TBootstrapDataset(int RowCount);

    const TTable& table_schema() const override;
    std::unique_ptr<IDatasetIterator> NewIterator() const override;

private:
    TTable BootstrapTable_;
    int RowCount_;
};

}  // namespace NYT::NTest
