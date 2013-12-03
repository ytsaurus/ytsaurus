#include "stdafx.h"
#include "compaction.h"
#include "config.h"
#include "tablet.h"
#include "static_memory_store.h"
#include "dynamic_memory_store.h"
#include "private.h"

namespace NYT {
namespace NTabletNode {

using namespace NVersionedTableClient;
using namespace NTransactionClient;

////////////////////////////////////////////////////////////////////////////////

static auto& Logger = TabletNodeLogger;

////////////////////////////////////////////////////////////////////////////////

class TMemoryCompactor::TStaticScanner
{
public:
    explicit TStaticScanner(TStaticMemoryStorePtr store)
        : Store_(std::move(store))
        , RowSize_(Store_->Data_->RowSize)
        , SegmentIt(Store_->Data_->Segments.begin())
        , RowIndexWithinSegment(0)
    {
        if (SegmentIt->RowCount == 0) {
            ++SegmentIt;
        }
    }

    bool IsValid() const
    {
        return SegmentIt != Store_->Data_->Segments.end();
    }

    TStaticRow GetCurrent() const
    {
        return TStaticRow(reinterpret_cast<TStaticRowHeader*>(
            SegmentIt->Rows.Begin() +
            RowIndexWithinSegment * RowSize_));
    }

    void Advance()
    {
        ++RowIndexWithinSegment;
        if (RowIndexWithinSegment == SegmentIt->RowCount) {
            ++SegmentIt;
           RowIndexWithinSegment = 0;
        }
    }

private:
    TStaticMemoryStorePtr Store_;

    size_t RowSize_;
    std::vector<TStaticMemoryStore::TSegment>::iterator SegmentIt;
    int RowIndexWithinSegment;

};

////////////////////////////////////////////////////////////////////////////////

TMemoryCompactor::TMemoryCompactor(
    TTabletManagerConfigPtr config,
    TTablet* tablet)
    : Config_(std::move(config))
    , Tablet_(tablet)
    , Comparer_(Tablet_->KeyColumns().size())
{ }

TStaticMemoryStorePtr TMemoryCompactor::Run(
    TDynamicMemoryStorePtr dynamicStore,
    TStaticMemoryStorePtr staticStore)
{
    LOG_INFO("Memory compaction started (TabletId: %s)",
        ~ToString(Tablet_->GetId()));
    
    TStaticMemoryStoreBuilder builder(Config_, Tablet_);

    TStaticScanner staticScanner(staticStore);

    TDynamicScanner dynamicScanner(dynamicStore->Tree_.get());
    dynamicScanner->BeginScan(EmptyKey());

    int keyCount = static_cast<int>(Tablet_->KeyColumns().size());
    int schemaColumnCount = static_cast<int>(Tablet_->Schema().Columns().size());


    auto beginRow = [&] (const TUnversionedValue* keys) {
        builder.BeginRow();
        auto* allocatedKeys = builder.AllocateKeys();
        ::memcpy(allocatedKeys, keys, sizeof(TUnversionedValue) * keyCount);
    };

    auto endRow = [&] (TTimestamp lastCommitTimestamp) {
        builder.EndRow(lastCommitTimestamp);
    };


    auto getStaticFixedValueCount = [&] (int index) -> int {
        if (!staticScanner.IsValid()) {
            return 0;
        }
        auto row = staticScanner.GetCurrent();
        return row.GetFixedValueCount(index, keyCount, schemaColumnCount);
    };

    auto getDynamicFixedValueCount = [&] (int index) -> int {
        if (!dynamicScanner->IsValid()) {
            return 0;
        }
        auto row = dynamicScanner->GetCurrent();
        auto list = row.GetFixedValueList(index, keyCount);
        return list ? list.GetSize() + list.GetSuccessorsSize() : 0;
    };

    auto copyStaticFixedValues = [&] (int index, TVersionedValue* values) {
        auto row = staticScanner.GetCurrent();
        int count = row.GetFixedValueCount(index, keyCount, schemaColumnCount);
        ::memcpy(values, row.GetFixedValues(index, keyCount), sizeof(TVersionedValue) * count);
    };

    std::vector<TValueList> valueLists;
    auto copyDynamicFixedValues = [&] (int index, TVersionedValue* values) {
        auto row = dynamicScanner->GetCurrent();
        
        valueLists.clear();
        auto currentList = row.GetFixedValueList(index, keyCount);
        while (currentList) {
            valueLists.push_back(currentList);
            currentList = currentList.GetNext();
        }

        for (auto it = valueLists.rbegin(); it != valueLists.rend(); ++it) {
            auto list = *it;
            ::memcpy(values, list.Begin(), sizeof(TVersionedValue) * list.GetSize());
            values += list.GetSize();
        }
    };


    auto getStaticTimestampCount = [&] () {
        if (!staticScanner.IsValid()) {
            return 0;
        }
        auto row = staticScanner.GetCurrent();
        return row.GetTimestampCount(keyCount, schemaColumnCount);
    };

    auto getDynamicTimestampCount = [&] () -> int {
        if (!dynamicScanner->IsValid()) {
            return 0;
        }
        auto row = dynamicScanner->GetCurrent();
        auto list = row.GetTimestampList(keyCount);
        return list ? list.GetSize() + list.GetSuccessorsSize() : 0;
    };

    auto copyStaticTimestamps = [&] (TTimestamp* timestamps) {
        auto row = staticScanner.GetCurrent();
        int count = row.GetTimestampCount(keyCount, schemaColumnCount);
        ::memcpy(timestamps, row.GetTimestamps(keyCount), sizeof(TTimestamp) * count);
    };

    std::vector<TTimestampList> timestampLists;
    auto copyDynamicTimestamps = [&] (TTimestamp* timestamps) {
        auto row = dynamicScanner->GetCurrent();
        
        valueLists.clear();
        auto currentList = row.GetTimestampList(keyCount);
        while (currentList) {
            timestampLists.push_back(currentList);
            currentList = currentList.GetNext();
        }

        for (auto it = timestampLists.rbegin(); it != timestampLists.rend(); ++it) {
            auto list = *it;
            ::memcpy(timestamps, list.Begin(), sizeof(TTimestamp) * list.GetSize());
            timestamps += list.GetSize();
        }
    };


    while (staticScanner.IsValid() || dynamicScanner->IsValid()) {
        int compareResult;
        if (staticScanner.IsValid() && dynamicScanner->IsValid()) {
            auto staticRow = staticScanner.GetCurrent();
            auto dynamicRow = dynamicScanner->GetCurrent();
            compareResult = Comparer_(staticRow, dynamicRow);
        } else if (staticScanner.IsValid()) {
            compareResult = -1;
        } else {
            compareResult = +1;
        }

        beginRow(
            compareResult < 0
            ? staticScanner.GetCurrent().GetKeys()
            : dynamicScanner->GetCurrent().GetKeys());

        for (int index = 0; index < schemaColumnCount - keyCount; ++index) {
            int staticCount = getStaticFixedValueCount(index);
            int dynamicCount = getDynamicFixedValueCount(index);
            auto* values = builder.AllocateFixedValues(index, staticCount + dynamicCount);
            if (staticCount > 0) {
                copyStaticFixedValues(index, values);
                values += staticCount;
            }
            if (dynamicCount > 0) {
                copyDynamicFixedValues(index, values);
                values += dynamicCount; // just for fun
            }
        }

        {
            int staticCount = getStaticTimestampCount();
            int dynamicCount = getDynamicTimestampCount();
            auto* timestamps = builder.AllocateTimestamps(staticCount + dynamicCount);
            if (staticCount > 0) {
                copyStaticTimestamps(timestamps);
                timestamps += staticCount;
            }
            if (dynamicCount > 0) {
                copyDynamicTimestamps(timestamps);
                timestamps += dynamicCount; // just for fun
            }
        }

        auto lastCommitTimestamp = NullTimestamp;
        if (compareResult <= 0) {
            lastCommitTimestamp = std::max(
                lastCommitTimestamp,
                staticScanner.GetCurrent().GetLastCommitTimestamp());
            staticScanner.Advance();
        }
        if (compareResult >= 0) {
            lastCommitTimestamp = std::max(
                lastCommitTimestamp,
                dynamicScanner->GetCurrent().GetLastCommitTimestamp());
            dynamicScanner->Advance();
        }

        endRow(lastCommitTimestamp);
    }

    LOG_INFO("Memory compaction completed (TabletId: %s)",
        ~ToString(Tablet_->GetId()));

    return builder.Finish();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
} // namespace NTabletNode
