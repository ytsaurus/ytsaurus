#pragma once

#include <yt/systest/dataset.h>

namespace NYT::NTest {

class TDecorateDataset : public IDataset
{
public:
    TDecorateDataset(std::unique_ptr<IDataset> inner, std::vector<TString> deletedColumnNames);

    virtual const TTable& table_schema() const override;
    virtual std::unique_ptr<IDatasetIterator> NewIterator() const override;

private:
    std::unique_ptr<IDataset> Inner_;
    TTable Table_;
};

}  // namespace NYT::NTest
