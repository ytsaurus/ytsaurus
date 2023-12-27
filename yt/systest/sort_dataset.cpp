
#include <library/cpp/yt/logging/logger.h>
#include <library/cpp/yson/node/node.h>

#include <yt/yt/core/misc/heap.h>

#include <yt/systest/util.h>

#include <yt/systest/sort_dataset.h>

namespace NYT::NTest {

using namespace NNodeCmp;

class TSortIterator : public IDatasetIterator
{
public:
    TSortIterator(const TSortDataset::TDataEntry* data, i64 NumEntries);

    TRange<TNode> Values() const override;
    bool Done() const override;
    void Next() override;

private:
    const TSortDataset::TDataEntry* Data_;
    const TSortDataset::TDataEntry* End_;
};

////////////////////////////////////////////////////////////////////////////////

class TMergeSortedDatasetIterator : public IDatasetIterator
{
public:
    TMergeSortedDatasetIterator(int sortColumns, std::vector<std::unique_ptr<IDatasetIterator>> iterators);

    TRange<TNode> Values() const override;
    bool Done() const override;
    void Next() override;

private:
    bool IteratorCmp(const IDatasetIterator* lhs, const IDatasetIterator* rhs);

    const int SortColumns_;
    std::vector<std::unique_ptr<IDatasetIterator>> Iterators_;
    std::vector<IDatasetIterator*> Heap_;
};

////////////////////////////////////////////////////////////////////////////////

static void ApplySortOperationInternal(
    const TTable& table,
    const TSortOperation& operation,
    TTable* output,
    std::vector<bool>* sortedMask,
    std::vector<int>* sortByIndices)
{
    sortedMask->clear();
    sortedMask->resize(std::ssize(table.DataColumns), false);
    std::unordered_map<TString, int> index;
    for (int i = 0; i < std::ssize(table.DataColumns); i++) {
        index.insert(std::make_pair(table.DataColumns[i].Name, i));
    }
    for (const auto& sortColumn : operation.SortBy) {
        auto position = index.find(sortColumn);
        if (position == index.end()) {
            THROW_ERROR_EXCEPTION("Sort column %v missing from input schema", sortColumn);
        }
        output->DataColumns.push_back(table.DataColumns[position->second]);
        output->DataColumns.back().StableName = std::nullopt;

        (*sortedMask)[position->second] = true;
        sortByIndices->push_back(position->second);
    }
    for (int i = 0; i < std::ssize(table.DataColumns); i++) {
        if ((*sortedMask)[i]) {
            continue;
        }
        output->DataColumns.push_back(table.DataColumns[i]);
    }
    output->SortColumns = std::ssize(operation.SortBy);
}

void ApplySortOperation(const TTable& table, const TSortOperation& operation, TTable* output) {
    std::vector<bool> sortedMask;
    std::vector<int> sortByIndices;
    ApplySortOperationInternal(table, operation, output, &sortedMask, &sortByIndices);
}

////////////////////////////////////////////////////////////////////////////////

TSortIterator::TSortIterator(const TSortDataset::TDataEntry* data, i64 NumEntries)
    : Data_(data)
    , End_(Data_ + NumEntries)
{
}

TRange<TNode> TSortIterator::Values() const
{
    return TRange<TNode>(Data_->Values);
}

bool TSortIterator::Done() const
{
    return Data_ == End_;
}

void TSortIterator::Next()
{
    ++Data_;
}

////////////////////////////////////////////////////////////////////////////////

TMergeSortedDatasetIterator::TMergeSortedDatasetIterator(
    int sortColumns,
    std::vector<std::unique_ptr<IDatasetIterator>> iterators)
    : SortColumns_(sortColumns)
    , Iterators_(std::move(iterators))
{
    for (auto& entry : Iterators_) {
        Heap_.push_back(entry.get());
    }
    MakeHeap(Heap_.begin(), Heap_.end(), BIND(&TMergeSortedDatasetIterator::IteratorCmp, this));
}

TRange<TNode> TMergeSortedDatasetIterator::Values() const
{
    return Heap_[0]->Values();
}

bool TMergeSortedDatasetIterator::Done() const
{
    return Heap_[0]->Done();
}

void TMergeSortedDatasetIterator::Next()
{
    Heap_[0]->Next();
    AdjustHeapFront(Heap_.begin(), Heap_.end(), BIND(&TMergeSortedDatasetIterator::IteratorCmp, this));
}

bool TMergeSortedDatasetIterator::IteratorCmp(const IDatasetIterator* lhs, const IDatasetIterator* rhs)
{
    if (lhs->Done() != rhs->Done()) {
        return lhs->Done() < rhs->Done();
    }
    if (lhs->Done()) {
        return false;
    }

    return CompareRowPrefix(SortColumns_, lhs->Values(), rhs->Values()) < 0;
}

////////////////////////////////////////////////////////////////////////////////

TSortDataset::TSortDataset(const IDataset& inner, const TSortOperation& operation)
    : Inner_(inner)
    , Operation_(operation)
    , NumColumns_(std::ssize(Inner_.table_schema().DataColumns))
{
    ApplySortOperationInternal(
        Inner_.table_schema(),
        Operation_,
        &Table_,
        &ColumnSorted_,
        &SortByIndices_);
    ConsumeAndSortInner();
}

std::vector<TNode> TSortDataset::BuildValues(TRange<TNode> values) const
{
    std::vector<TNode> result;
    for (int i = 0; i < std::ssize(SortByIndices_); i++) {
        result.push_back(values[SortByIndices_[i]]);
    }
    for (int i = 0; i < NumColumns_; i++) {
        if (ColumnSorted_[i]) {
            continue;
        }
        result.push_back(values[i]);
    }
    NYT::NLogging::TLogger Logger("test");
    return result;
}

void TSortDataset::ConsumeAndSortInner()
{
    for (auto iterator = Inner_.NewIterator(); !iterator->Done(); iterator->Next()) {
        Data_.push_back(TDataEntry{BuildValues(iterator->Values())});
    }

    std::sort(Data_.begin(), Data_.end(), [this](const TDataEntry& lhs, const TDataEntry& rhs) {
        return Comparator(lhs, rhs) < 0;
    });
}

int TSortDataset::Comparator(const TDataEntry& lhs, const TDataEntry& rhs) const
{
    return CompareRowPrefix(Table_.SortColumns, lhs.Values, rhs.Values);
}

const TTable& TSortDataset::table_schema() const
{
    return Table_;
}

std::unique_ptr<IDatasetIterator> TSortDataset::NewIterator() const
{
    return std::make_unique<TSortIterator>(&Data_[0], std::ssize(Data_));
}

////////////////////////////////////////////////////////////////////////////////

TMergeSortedDataset::TMergeSortedDataset(std::vector<const IDataset*> inner)
    : Inner_(inner)
{
    YT_VERIFY(std::ssize(Inner_) > 0);
}

const TTable& TMergeSortedDataset::table_schema() const
{
    return Inner_[0]->table_schema();
}

std::unique_ptr<IDatasetIterator> TMergeSortedDataset::NewIterator() const
{
    std::vector<std::unique_ptr<IDatasetIterator>> iterators;
    for (auto& dataset : Inner_) {
        iterators.push_back(dataset->NewIterator());
    }
    return std::make_unique<TMergeSortedDatasetIterator>(
        table_schema().SortColumns,
        std::move(iterators));
}

}  // namespace NYT::NTest
