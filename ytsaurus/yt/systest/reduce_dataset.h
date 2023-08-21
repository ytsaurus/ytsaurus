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

    struct DataEntry {
        std::vector<TNode> Values;
    };

    std::vector<DataEntry> Data_;

    std::vector<int> SortColumnIndices_;
    std::vector<int> DataEntryIndex_;

    void ComputeSortColumnIndices();
    void ConsumeAndSortInner();

    int SortColumnsComparator(int lhs, int rhs) const;

    const IDataset& Inner_;
    const TReduceOperation& Operation_;
    const TTable Table_;
};

}  // namespace NYT::NTest
