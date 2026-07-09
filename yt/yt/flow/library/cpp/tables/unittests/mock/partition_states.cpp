#include "partition_states.h"

#include <yt/yt/flow/library/cpp/tables/state.h>

#include <yt/yt/flow/lib/serializer/state.h>

#include <yt/yt/client/table_client/row_buffer.h>
#include <yt/yt/client/table_client/unversioned_row.h>

namespace NYT::NFlow::NTables {

using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

bool TInMemoryPartitionStates::TTableKeyCompare::operator()(const TTableKey& a, const TTableKey& b) const
{
    if (a.PartitionId != b.PartitionId) {
        return a.PartitionId < b.PartitionId;
    }
    return a.Name < b.Name;
}

////////////////////////////////////////////////////////////////////////////////

TFuture<THashMap<IPartitionStates::TTableKey, NYsonSerializer::TStatePtr>>
TInMemoryPartitionStates::Lookup(THashSet<TTableKey> keys, std::optional<std::string> /*tag*/)
{
    const auto stateSchema = NYsonSerializer::GetYsonStateSchema<TInternalState>();
    THashMap<TTableKey, NYsonSerializer::TStatePtr> result;

    for (const auto& tableKey : keys) {
        auto state = New<NYsonSerializer::TState>(stateSchema);
        auto it = Storage_.find(tableKey);
        if (it != Storage_.end()) {
            // Pass the stored full row directly to Init.
            const auto& storedRow = it->second;
            auto rowBuffer = New<TRowBuffer>();
            TUnversionedRowBuilder builder;
            for (const auto& ownedValue : storedRow) {
                builder.AddValue(rowBuffer->CaptureValue(ownedValue));
            }
            state->Init(builder.GetRow());
        } else {
            state->Init();
        }
        result[tableKey] = std::move(state);
    }

    return MakeFuture(std::move(result));
}

void TInMemoryPartitionStates::Write(
    NApi::IDynamicTableTransactionPtr /*transaction*/,
    const THashMap<TTableKey, NYsonSerializer::TStateMutation>& mutations,
    std::optional<std::string> /*tag*/)
{
    for (const auto& [tableKey, mutation] : mutations) {
        if (const auto* update = std::get_if<NYsonSerializer::TUpdateMutation>(&mutation)) {
            // TUpdateMutation is a partial row: only modified columns.
            // Merge into the stored full row by column id.
            auto& storedRow = Storage_[tableKey];
            for (const auto& value : *update) {
                // Ensure the vector is large enough for this column id.
                if (static_cast<i64>(storedRow.size()) <= value.Id) {
                    // Fill gaps with proper Null values.
                    while (static_cast<i64>(storedRow.size()) < value.Id) {
                        auto nullValue = MakeUnversionedNullValue(static_cast<int>(storedRow.size()));
                        storedRow.push_back(TUnversionedOwningValue(nullValue));
                    }
                    storedRow.push_back(TUnversionedOwningValue(value));
                } else {
                    storedRow[value.Id] = TUnversionedOwningValue(value);
                }
            }
            ++WriteCount_;
            ++WrittenKeyCount_;
        } else if (std::get_if<NYsonSerializer::TEraseMutation>(&mutation)) {
            Storage_.erase(tableKey);
            ++WriteCount_;
            ++WrittenKeyCount_;
        } else {
            YT_VERIFY(std::get_if<NYsonSerializer::TEmptyMutation>(&mutation));
        }
    }
}

TFuture<IPartitionStates::TListResult> TInMemoryPartitionStates::List(
    TTableKeyFilter filter,
    i64 limit,
    std::optional<TTableKey> offsetExclusive)
{
    TListResult result;
    i64 count = 0;

    for (const auto& [tableKey, storedRow] : Storage_) {
        if (filter.PartitionId && tableKey.PartitionId != *filter.PartitionId) {
            continue;
        }
        if (filter.Name && tableKey.Name != *filter.Name) {
            continue;
        }
        if (offsetExclusive) {
            if (!TTableKeyCompare{}(*offsetExclusive, tableKey)) {
                continue;
            }
        }
        if (count >= limit) {
            result.ContinuationOffsetExclusive = tableKey;
            break;
        }
        result.Keys.push_back(tableKey);
        ++count;
    }

    return MakeFuture(std::move(result));
}

TFuture<std::vector<IPartitionStates::TTableKey>> TInMemoryPartitionStates::ListAll(TTableKeyFilter filter)
{
    std::vector<TTableKey> result;
    std::optional<TTableKey> offsetExclusive;
    constexpr i64 BatchSize = 1000;
    while (true) {
        auto batchResult = NConcurrency::WaitFor(List(filter, BatchSize, offsetExclusive)).ValueOrThrow();
        result.insert(result.end(), batchResult.Keys.begin(), batchResult.Keys.end());
        if (!batchResult.ContinuationOffsetExclusive) {
            break;
        }
        offsetExclusive = batchResult.ContinuationOffsetExclusive;
    }
    return MakeFuture(std::move(result));
}

i64 TInMemoryPartitionStates::GetWriteCount() const
{
    return WriteCount_;
}

i64 TInMemoryPartitionStates::GetWrittenKeyCount() const
{
    return WrittenKeyCount_;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NTables
