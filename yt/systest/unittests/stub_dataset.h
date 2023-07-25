#pragma once

#include <yt/systest/dataset.h>

namespace NYT::NTest {

class TStubDataset : public IDataset {
public:
    TStubDataset(const TTable& table, std::vector<std::vector<TNode>> data);
    virtual const TTable& table_schema() const;
    virtual std::unique_ptr<IDatasetIterator> NewIterator() const;

private:
    const TTable& Table_;
    std::vector<std::vector<TNode>> Data_;
};

}  // namespace NYT::NTest
