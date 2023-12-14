#pragma once

#include <yt/systest/dataset.h>

namespace NYT::NTest {

void ApplySortOperation(const TTable& table, const TSortOperation& operation, TTable* output);

class TSortDataset : public IDataset
{
public:
    TSortDataset(const IDataset& inner, const TSortOperation& operation);

    virtual const TTable& table_schema() const;
    virtual std::unique_ptr<IDatasetIterator> NewIterator() const;

private:
    friend class TSortIterator;

    struct TDataEntry {
        std::vector<TNode> Values;
    };

    std::vector<TDataEntry> Data_;

    void BuildOutputTableSchema();
    void ConsumeAndSortInner();

    std::vector<TNode> BuildValues(TRange<TNode> values) const;

    int Comparator(const TDataEntry& lhs, const TDataEntry& rhs) const;
    const IDataset& Inner_;
    TSortOperation Operation_;

    std::vector<int> SortByIndices_;
    const int NumColumns_;
    std::vector<bool> ColumnSorted_;
    TTable Table_;
};

///////////////////////////////////////////////////////////////////////////////

class TMergeSortedDataset : public IDataset
{
public:
    // Every inner dataset must have the same table schema.
    TMergeSortedDataset(std::vector<const IDataset*> inner);

    virtual const TTable& table_schema() const;
    virtual std::unique_ptr<IDatasetIterator> NewIterator() const;

private:
    friend class TMergeSortedDatasetIterator;

    std::vector<const IDataset*> Inner_;
};

}  // namespace NYT::NTest
