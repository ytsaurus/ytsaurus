#pragma once

#include <yt/systest/dataset.h>

namespace NYT::NTest {

class IMultiRowOperation;

class MapDataset : public IDataset
{
public:
    MapDataset(const IDataset& inner, const IMultiMapper& operation);

    virtual const TTable& table_schema() const;
    virtual std::unique_ptr<IDatasetIterator> NewIterator() const;

private:
    const IDataset& Inner_;
    const IMultiMapper& Operation_;
    std::vector<TString> ColumnStrings_;
    TTable Table_;
};

}  // namespace NYT::NTest
