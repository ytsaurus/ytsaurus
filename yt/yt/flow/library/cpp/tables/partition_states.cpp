#include "partition_states.h"

#include "common.h"
#include "context.h"
#include "state.h"

#include <yt/yt/flow/library/cpp/common/spec.h>

#include <yt/yt/flow/library/cpp/misc/load_throughput_throttler.h>

#include <yt/yt/flow/library/cpp/serializer/state.h>

#include <yt/yt/core/profiling/timing.h>

#include <yt/yt/client/api/client.h>
#include <yt/yt/client/api/dynamic_table_client.h>
#include <yt/yt/client/api/transaction.h>

#include <yt/yt/client/table_client/helpers.h>
#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/row_buffer.h>
#include <yt/yt/client/table_client/unversioned_row.h>

#include <yt/yt/core/ypath/helpers.h>

#include <util/string/join.h>

namespace NYT::NFlow::NTables {

using namespace NApi;
using namespace NConcurrency;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

// IPartitionStates non-virtual convenience methods.

TFuture<NYsonSerializer::TStatePtr> IPartitionStates::Lookup(TTableKey key, std::optional<std::string> tag)
{
    return Lookup(THashSet<TTableKey>{std::move(key)}, std::move(tag))
        .AsUnique()
        .Apply(BIND([] (TErrorOr<THashMap<TTableKey, NYsonSerializer::TStatePtr>>&& result) -> NYsonSerializer::TStatePtr {
            auto states = std::move(result.ValueOrThrow());
            YT_VERIFY(std::ssize(states) == 1);
            return std::move(states.begin()->second);
        }));
}

void IPartitionStates::Write(
    NApi::IDynamicTableTransactionPtr transaction,
    const TTableKey& key,
    const NYsonSerializer::TStateMutation& mutation,
    std::optional<std::string> tag)
{
    Write(transaction, THashMap<TTableKey, NYsonSerializer::TStateMutation>{{key, mutation}}, std::move(tag));
}

void IPartitionStates::Erase(
    NApi::IDynamicTableTransactionPtr transaction,
    const THashSet<TTableKey>& keys)
{
    THashMap<TTableKey, NYsonSerializer::TStateMutation> mutations;
    for (const auto& key : keys) {
        mutations[key] = NYsonSerializer::TEraseMutation{};
    }
    Write(transaction, mutations);
}

void IPartitionStates::Erase(
    NApi::IDynamicTableTransactionPtr transaction,
    const std::vector<TTableKey>& keys)
{
    Erase(transaction, THashSet<TTableKey>(keys.begin(), keys.end()));
}

////////////////////////////////////////////////////////////////////////////////

TPartitionStates::TPartitionStates(
    TContextPtr context,
    TDynamicTableRequestSpecPtr dynamicSpec,
    std::optional<std::string> defaultTag)
    : Context_(context->WithTableName(PartitionStatesTableName))
    , DynamicSpec_(std::move(dynamicSpec))
    , TablePath_(NYPath::YPathJoin(Context_->PipelinePath.GetPath(), PartitionStatesTableName))
    , Logger(Context_->Logger)
    , DefaultTag_(defaultTag.value_or(std::string(PartitionStatesTableName)))
{ }

TPartitionStates::TTagMetrics& TPartitionStates::GetOrCreateTagMetrics(const std::string& tag)
{
    auto guard = Guard(TagMetricsLock_);
    auto [it, inserted] = TagMetrics_.try_emplace(tag);
    auto& metrics = it->second;
    if (inserted) {
        auto profiler = Context_->Profiler.WithTag("tag", tag);
        metrics.LookupRows = profiler.Counter("/lookup_rows");
        metrics.LookupBytes = profiler.Counter("/lookup_bytes");
        metrics.SelectRows = profiler.Counter("/select_rows");
        metrics.SelectBytes = profiler.Counter("/select_bytes");
        metrics.WriteRows = profiler.Counter("/write_rows");
        metrics.WriteBytes = profiler.Counter("/write_bytes");
        metrics.UpdateRows = profiler.Counter("/update_rows");
        metrics.EraseRows = profiler.Counter("/erase_rows");
        metrics.LookupTime = profiler.Timer("/lookup_time");
        metrics.SelectTime = profiler.Timer("/select_time");
    }
    return metrics;
}

void TPartitionStates::Reconfigure(TDynamicTableRequestSpecPtr dynamicSpec)
{
    DynamicSpec_ = std::move(dynamicSpec);
}

TFuture<THashMap<TPartitionStates::TTableKey, NYsonSerializer::TStatePtr>> TPartitionStates::Lookup(
    THashSet<TTableKey> keys,
    std::optional<std::string> tag)
{
    auto effectiveTag = tag.value_or(DefaultTag_);
    return BIND([strongThis = MakeStrong(this), this, keys = std::move(keys), effectiveTag = std::move(effectiveTag)] () {
        WaitFor(Context_->LoadThroughputThrottler->ThrottleRows(effectiveTag, std::ssize(keys))).ThrowOnError();

        auto nameTable = New<TNameTable>();
        TUnversionedRowsBuilder builder;
        THashMap<TTableKey, NYsonSerializer::TStatePtr> states;
        const auto stateSchema = NYsonSerializer::GetYsonStateSchema<TInternalState>();
        {
            const i32 partitionIdField = nameTable->GetIdOrRegisterName("partition_id");
            const i32 nameField = nameTable->GetIdOrRegisterName("name");
            for (const auto& key : keys) {
                states[key] = New<NYsonSerializer::TState>(stateSchema);
                builder.AddRow(
                    TAnnotatedValue(ToString(key.PartitionId), partitionIdField),
                    TAnnotatedValue(key.Name, nameField));
            }
        }
        TLookupRowsOptions lookupRowsOptions;
        lookupRowsOptions.Timestamp = NTransactionClient::SyncLastCommittedTimestamp;

        NProfiling::TWallTimer lookupTimer;
        const auto result = WaitFor(Context_->Client->LookupRows(TablePath_, nameTable, builder.Build(), lookupRowsOptions)).ValueOrThrow();
        auto lookupElapsed = lookupTimer.GetElapsedTime();
        i64 totalLookupRows = 0;
        i64 totalLookupBytes = 0;
        {
            std::vector<i64> sizes;
            const auto rowset = result.Rowset;
            const auto schema = rowset->GetSchema();
            const auto mapping = NYsonSerializer::PrepareMapping(stateSchema, schema);
            const i32 partitionIdField = schema->GetColumnIndexOrThrow("partition_id");
            const i32 nameField = schema->GetColumnIndexOrThrow("name");
            for (const auto& row : rowset->GetRows()) {
                if (row) {
                    const auto partitionId = FromUnversionedValue<TPartitionId>(row[partitionIdField]);
                    const auto name = FromUnversionedValue<std::string>(row[nameField]);
                    states[TTableKey{partitionId, name}]->Init(row, mapping);
                    auto weight = GetDataWeight(row);
                    sizes.push_back(weight);
                    ++totalLookupRows;
                    totalLookupBytes += weight;
                }
            }
            Context_->LoadThroughputThrottler->RegisterRows(effectiveTag, sizes);
        }
        auto& metrics = GetOrCreateTagMetrics(effectiveTag);
        metrics.LookupRows.Increment(totalLookupRows);
        metrics.LookupBytes.Increment(totalLookupBytes);
        metrics.LookupTime.Record(lookupElapsed);
        return states;
    })
        .AsyncVia(GetCurrentInvoker())
        .Run();
}

void TPartitionStates::Write(
    NApi::IDynamicTableTransactionPtr transaction,
    const THashMap<TTableKey, NYsonSerializer::TStateMutation>& mutations,
    std::optional<std::string> tag)
{
    if (mutations.empty()) {
        return;
    }

    auto effectiveTag = tag.value_or(DefaultTag_);

    const auto stateSchema = NYsonSerializer::GetYsonStateSchema<TInternalState>();
    auto nameTable = TNameTable::FromSchema(*stateSchema->TableSchema);
    const i32 partitionIdField = nameTable->GetIdOrRegisterName("partition_id");
    const i32 nameField = nameTable->GetIdOrRegisterName("name");

    i64 writeRowCount = 0;
    i64 deleteRowCount = 0;
    auto rowBuffer = New<TRowBuffer>();
    std::vector<TRowModification> rows;
    rows.reserve(mutations.size());
    for (const auto& [tableKey, mutation] : mutations) {
        TUnversionedRowBuilder builder;
        const auto partitionId = ToString(tableKey.PartitionId);
        builder.AddValue(MakeUnversionedStringValue(partitionId, partitionIdField));
        builder.AddValue(MakeUnversionedStringValue(tableKey.Name, nameField));
        if (const auto* update = std::get_if<NYsonSerializer::TUpdateMutation>(&mutation)) {
            for (const auto& value : *update) {
                builder.AddValue(value);
            }
            auto row = rowBuffer->CaptureRow(builder.GetRow(), /*captureValues*/ true);
            rows.push_back(NRowModifications::TWriteRow(row));
            ++writeRowCount;
        } else if (std::get_if<NYsonSerializer::TEraseMutation>(&mutation)) {
            auto row = rowBuffer->CaptureRow(builder.GetRow(), /*captureValues*/ true);
            rows.push_back(NRowModifications::TDeleteRow(row));
            ++deleteRowCount;
        } else {
            YT_VERIFY(std::get_if<NYsonSerializer::TEmptyMutation>(&mutation));
        }
    }
    auto writeBytes = rowBuffer->GetSize();
    transaction->ModifyRows(TablePath_, nameTable, MakeSharedRange(std::move(rows), std::move(rowBuffer)));

    auto& metrics = GetOrCreateTagMetrics(effectiveTag);
    metrics.WriteRows.Increment(writeRowCount + deleteRowCount);
    metrics.WriteBytes.Increment(writeBytes);
    metrics.UpdateRows.Increment(writeRowCount);
    metrics.EraseRows.Increment(deleteRowCount);
}

TFuture<TPartitionStates::TListResult> TPartitionStates::List(TTableKeyFilter filter, i64 limit, std::optional<TTableKey> offsetExclusive)
{
    return BIND([this, strongThis = MakeStrong(this), filter, limit, offsetExclusive] () {
        WaitFor(Context_->LoadThroughputThrottler->ThrottleKeys(DefaultTag_, limit)).ThrowOnError();
        std::vector<std::string> conditions;
        if (filter.PartitionId) {
            conditions.push_back(Format("partition_id = %Qv", *filter.PartitionId));
        }
        if (filter.Name) {
            conditions.push_back(Format("name = %Qv",
                *filter.Name));
        }
        if (offsetExclusive) {
            conditions.push_back(Format("(partition_id, name) > (%Qv, %Qv)",
                offsetExclusive->PartitionId,
                offsetExclusive->Name));
        }

        auto query = Format("partition_id, name from [%v] %v limit %v",
            TablePath_,
            conditions.empty() ? "" : std::string("where ") + JoinSeq(" and ", conditions),
            limit);
        TSelectRowsOptions selectRowsOptions;
        selectRowsOptions.Timestamp = NTransactionClient::SyncLastCommittedTimestamp;
        NProfiling::TWallTimer selectTimer;
        auto selectResult = WaitFor(Context_->Client->SelectRows(query, selectRowsOptions)).ValueOrThrow();
        auto selectElapsed = selectTimer.GetElapsedTime();
        const auto rowset = selectResult.Rowset;

        TListResult result;
        result.Keys.reserve(limit);
        i64 totalSelectRows = 0;
        i64 totalSelectBytes = 0;
        {
            std::vector<i64> sizes;
            const auto schema = rowset->GetSchema();
            const i32 partitionIdField = schema->GetColumnIndexOrThrow("partition_id");
            const i32 nameField = schema->GetColumnIndexOrThrow("name");
            for (const auto& row : rowset->GetRows()) {
                if (row) {
                    const auto partitionId = FromUnversionedValue<TPartitionId>(row[partitionIdField]);
                    const auto name = FromUnversionedValue<std::string>(row[nameField]);
                    auto tableKey = TTableKey{partitionId, name};
                    result.Keys.push_back(tableKey);
                    auto weight = GetDataWeight(row);
                    sizes.push_back(weight);
                    ++totalSelectRows;
                    totalSelectBytes += weight;
                }
            }
            Context_->LoadThroughputThrottler->RegisterKeys(DefaultTag_, sizes);
        }
        if (std::ssize(rowset->GetRows()) == limit) {
            result.ContinuationOffsetExclusive = result.Keys.back();
        }
        auto& metrics = GetOrCreateTagMetrics(DefaultTag_);
        metrics.SelectRows.Increment(totalSelectRows);
        metrics.SelectBytes.Increment(totalSelectBytes);
        metrics.SelectTime.Record(selectElapsed);
        return result;
    })
        .AsyncVia(GetCurrentInvoker())
        .Run();
}

TFuture<std::vector<TPartitionStates::TTableKey>> TPartitionStates::ListAll(TTableKeyFilter filter)
{
    return BIND([this, strongThis = MakeStrong(this), filter] () {
        std::vector<TTableKey> keys;
        std::optional<TTableKey> offsetExclusive;
        TSelectLimiter limiter(DynamicSpec_);
        while (true) {
            auto result = WaitFor(List(filter, limiter.Get(), offsetExclusive)).ValueOrThrow();
            keys.insert(keys.end(), result.Keys.begin(), result.Keys.end());
            if (!result.ContinuationOffsetExclusive) {
                break;
            }
            offsetExclusive = result.ContinuationOffsetExclusive;
        }
        return keys;
    })
        .AsyncVia(GetCurrentInvoker())
        .Run();
}

////////////////////////////////////////////////////////////////////////////////

TTaggedPartitionStates::TTaggedPartitionStates(IPartitionStatesPtr table, std::string tag)
    : Table_(std::move(table))
    , Tag_(std::move(tag))
{ }

void TTaggedPartitionStates::Reconfigure(TDynamicTableRequestSpecPtr dynamicSpec)
{
    Table_->Reconfigure(std::move(dynamicSpec));
}

TFuture<THashMap<TTaggedPartitionStates::TTableKey, NYsonSerializer::TStatePtr>> TTaggedPartitionStates::Lookup(
    THashSet<TTableKey> keys,
    std::optional<std::string> tag)
{
    return Table_->Lookup(std::move(keys), tag ? std::move(tag) : Tag_);
}

void TTaggedPartitionStates::Write(
    NApi::IDynamicTableTransactionPtr transaction,
    const THashMap<TTableKey, NYsonSerializer::TStateMutation>& mutations,
    std::optional<std::string> tag)
{
    Table_->Write(transaction, mutations, tag ? std::move(tag) : Tag_);
}

TFuture<TTaggedPartitionStates::TListResult> TTaggedPartitionStates::List(
    TTableKeyFilter filter,
    i64 limit,
    std::optional<TTableKey> offsetExclusive)
{
    return Table_->List(std::move(filter), limit, std::move(offsetExclusive));
}

TFuture<std::vector<TTaggedPartitionStates::TTableKey>> TTaggedPartitionStates::ListAll(TTableKeyFilter filter)
{
    return Table_->ListAll(std::move(filter));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NTables

size_t THash<NYT::NFlow::NTables::IPartitionStates::TTableKey>::operator()(const NYT::NFlow::NTables::IPartitionStates::TTableKey& tableKey) const
{
    auto value = std::tuple(tableKey.PartitionId, tableKey.Name);
    return THash<decltype(value)>()(value);
}
