#include "compaction.h"
#include "tablet.h"

namespace NYT::NLsm::NTesting {

using namespace NTransactionClient;

////////////////////////////////////////////////////////////////////////////////

class TRowMerger
{
public:
    void AddRow(const TRow& row)
    {
        Result_.Key = row.Key;
        Result_.Values.insert(
            Result_.Values.end(),
            row.Values.begin(),
            row.Values.end());
        Result_.DeleteTimestamps.insert(
            Result_.DeleteTimestamps.end(),
            row.DeleteTimestamps.begin(),
            row.DeleteTimestamps.end());
        Result_.DataSize += row.DataSize;
    }

    TRow GetMergedRow(TTimestamp retentionTimestamp, TTimestamp majorTimestamp, int minDataVersions)
    {
        YT_VERIFY(!Built_);
        Built_ = true;

        auto& values = Result_.Values;
        std::sort(
            values.begin(),
            values.end(),
            [&] (const auto& lhs, const auto& rhs) {
                return lhs.Timestamp > rhs.Timestamp;
            });
        auto it = values.begin();
        while (it != values.end() && it->Timestamp > retentionTimestamp) {
            ++it;
        }

        auto& deleteTimestamps = Result_.DeleteTimestamps;
        std::sort(
            deleteTimestamps.begin(),
            deleteTimestamps.end(),
            [&] (const auto& lhs, const auto& rhs) {
                return lhs > rhs;
            });
        auto tombstoneIt = deleteTimestamps.begin();
        while (tombstoneIt != deleteTimestamps.end() && *tombstoneIt > retentionTimestamp) {
            ++tombstoneIt;
        }

        if ((it != values.end() || tombstoneIt != deleteTimestamps.end()) && minDataVersions > 0) {
            if (tombstoneIt == deleteTimestamps.end()) {
                ++it;
            } else if (it == values.end()) {
                ++tombstoneIt;
            } else {
                if (it->Timestamp > *tombstoneIt) {
                    ++it;
                } else {
                    ++tombstoneIt;
                }
            }
        }

        while (tombstoneIt != deleteTimestamps.end() && *tombstoneIt >= majorTimestamp) {
            ++tombstoneIt;
        }

        if (it != values.end()) {
            values.erase(it, values.end());
        }
        if (tombstoneIt != deleteTimestamps.end()) {
            deleteTimestamps.erase(tombstoneIt, deleteTimestamps.end());
        }

        i64 dataSize = 0;
        for (auto value : values) {
            dataSize += value.DataSize;
        }
        Result_.DataSize = dataSize;

        return Result_;
    }

private:
    TRow Result_;
    bool Built_ = false;

};

TTimestamp ComputeMajorTimestamp(
    TPartition* partition,
    const std::vector<TStore*>& stores)
{
    auto result = MaxTimestamp;
    auto handleStore = [&] (const TStore* store) {
        result = std::min(result, store->GetMinTimestamp());
    };

    auto* tablet = partition->GetTablet();

    for (const auto& store : tablet->Eden()->Stores()) {
        handleStore(store.get());
    }

    for (const auto& store : partition->Stores()) {
        if (store->GetType() == EStoreType::SortedChunk) {
            if (std::find(stores.begin(), stores.end(), store.get()) == stores.end()) {
                handleStore(store.get());
            }
        }
    }

    return result;
}

std::vector<std::unique_ptr<TStore>> Compact(
    const std::vector<TStore*>& stores,
    TPartition* partition,
    const std::vector<TKey>& pivotKeys,
    TTimestamp retentionTimestamp,
    TTimestamp majorTimestamp,
    i64 minDataVersions,
    double compressionRatio)
{
    struct TStorePosition
    {
        TStore* Store;
        std::vector<TRow>::const_iterator Iterator;

        TKey GetKey() const
        {
            return Iterator->Key;
        }
        TRow GetRow() const
        {
            return *Iterator;
        }

        bool Advance()
        {
            ++Iterator;
            return Iterator != Store->Rows().end();
        }

        bool operator<(const TStorePosition& other) const
        {
            return Iterator->Key < other.Iterator->Key;
        }

        bool operator>(const TStorePosition& other) const
        {
            return other < *this;
        }
    };

    std::priority_queue<TStorePosition, std::vector<TStorePosition>, std::greater<TStorePosition>> queue;
    for (auto* store : stores) {
        queue.push({store, store->Rows().begin()});
    }

    auto popAndAdvance = [&] {
        auto storePosition = queue.top();
        queue.pop();
        if (storePosition.Advance()) {
            queue.push(storePosition);
        }
    };

    YT_VERIFY(!pivotKeys.empty());
    auto pivotIt = std::next(pivotKeys.begin());

    std::vector<std::unique_ptr<TStore>> result;
    std::vector<TRow> currentStore;

    auto flushPartition = [&] {
        if (currentStore.empty()) {
            result.emplace_back();
            return;
        }
        auto store = MakeStore(std::move(currentStore), compressionRatio);
        store->SetPartition(partition);
        store->SetIsCompactable(true);
        result.push_back(std::move(store));
        currentStore.clear();
    };

    while (!queue.empty()) {
        auto key = queue.top().GetKey();
        TRowMerger merger;
        while (!queue.empty() && queue.top().GetKey() == key) {
            merger.AddRow(queue.top().GetRow());
            popAndAdvance();
        }

        while (pivotIt != pivotKeys.end() && key >= *pivotIt) {
            flushPartition();
            ++pivotIt;
        }

        auto mergedRow = merger.GetMergedRow(retentionTimestamp, majorTimestamp, minDataVersions);
        if (!mergedRow.Values.empty() || !mergedRow.DeleteTimestamps.empty()) {
            currentStore.push_back(mergedRow);
        }
    }

    while (pivotIt != pivotKeys.end()) {
        flushPartition();
        ++pivotIt;
    }
    flushPartition();

    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLsm::NTesting
