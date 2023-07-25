#pragma once

#include <yt/systest/dataset.h>

namespace NYT::NTest {

class TReduceDataset : public IDataset
{
public:
    TReduceDataset(const IDataset& inner, const TReduceOperation& operation);

    virtual const TTable& table_schema() const;
    virtual std::unique_ptr<IDatasetIterator> NewIterator() const;

private:
    friend class TReduceDatasetIterator;

    std::vector<int> ReduceByIndices_;
    std::vector<int> DataEntryIndex_;

    void ComputeColumnIndices();
    bool ReduceByEqual(const std::vector<TNode>& lhs, TRange<TNode> rhs) const;

    const IDataset& Inner_;
    const TReduceOperation& Operation_;
    TTable Table_;
};

}  // namespace NYT::NTest
