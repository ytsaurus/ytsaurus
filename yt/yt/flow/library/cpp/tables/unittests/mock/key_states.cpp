#include "key_states.h"

#include <yt/yt/flow/library/cpp/tables/state.h>

#include <yt/yt/flow/lib/serializer/state.h>

#include <yt/yt/client/table_client/row_buffer.h>
#include <yt/yt/client/table_client/unversioned_row.h>

namespace NYT::NFlow::NTables {

using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

bool TInMemoryKeyStates::TTableKeyCompare::operator()(const TTableKey& a, const TTableKey& b) const
{
    if (a.ComputationId != b.ComputationId) {
        return a.ComputationId < b.ComputationId;
    }
    if (a.Key != b.Key) {
        return a.Key < b.Key;
    }
    return a.Name < b.Name;
}

////////////////////////////////////////////////////////////////////////////////

TFuture<THashMap<IKeyStates::TTableKey, NYsonSerializer::TStatePtr>>
TInMemoryKeyStates::Lookup(THashSet<TTableKey> keys, std::optional<std::string> /*tag*/)
{
    ++LookupCount_;
    LoadedKeyCount_ += std::ssize(keys);
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

void TInMemoryKeyStates::Write(
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

TFuture<IKeyStates::TListResult> TInMemoryKeyStates::List(
    TTableKeyFilter filter,
    i64 limit,
    std::optional<TTableKey> offsetExclusive)
{
    if (ListFailure_) {
        return MakeFuture<TListResult>(TError(*ListFailure_));
    }

    TListResult result;
    i64 count = 0;

    for (const auto& [tableKey, storedRow] : Storage_) {
        if (filter.ComputationId && tableKey.ComputationId != *filter.ComputationId) {
            continue;
        }
        if (filter.ExactKey && tableKey.Key != *filter.ExactKey) {
            continue;
        }
        if (filter.LowerKey && tableKey.Key < *filter.LowerKey) {
            continue;
        }
        if (filter.UpperKey && *filter.UpperKey < tableKey.Key) {
            // Mirrors the production query (`key <= UpperKey`).
            continue;
        }
        if (filter.Names) {
            if (std::find(filter.Names->begin(), filter.Names->end(), tableKey.Name) == filter.Names->end()) {
                continue;
            }
        } else if (filter.Name && tableKey.Name != *filter.Name) {
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

TFuture<std::vector<IKeyStates::TTableKey>> TInMemoryKeyStates::ListAll(TTableKeyFilter filter)
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

void TInMemoryKeyStates::Set(TTableKey tableKey, std::vector<TUnversionedOwningValue> values)
{
    Storage_[std::move(tableKey)] = std::move(values);
}

i64 TInMemoryKeyStates::GetWriteCount() const
{
    return WriteCount_;
}

i64 TInMemoryKeyStates::GetWrittenKeyCount() const
{
    return WrittenKeyCount_;
}

i64 TInMemoryKeyStates::GetLookupCount() const
{
    return LookupCount_;
}

i64 TInMemoryKeyStates::GetLoadedKeyCount() const
{
    return LoadedKeyCount_;
}

void TInMemoryKeyStates::SetListFailure(std::optional<TError> error)
{
    ListFailure_ = std::move(error);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NTables
