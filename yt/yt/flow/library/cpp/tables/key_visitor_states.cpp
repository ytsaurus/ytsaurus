#include "key_visitor_states.h"

#include "common.h"
#include "context.h"

#include <yt/yt/flow/library/cpp/common/key.h>
#include <yt/yt/flow/library/cpp/common/spec.h>

#include <yt/yt/flow/library/cpp/misc/load_throughput_throttler.h>

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
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

TKeyVisitorStates::TKeyVisitorStates(
    TContextPtr context,
    TDynamicTableRequestSpecPtr dynamicSpec)
    : Context_(context->WithTableName(KeyVisitorStatesTableName))
    , DynamicSpec_(std::move(dynamicSpec))
    , TablePath_(NYPath::YPathJoin(Context_->PipelinePath.GetPath(), KeyVisitorStatesTableName))
    , Logger(Context_->Logger)
    , SelectRows_(Context_->Profiler.Counter("/select_rows"))
    , SelectBytes_(Context_->Profiler.Counter("/select_bytes"))
    , WriteRows_(Context_->Profiler.Counter("/write_rows"))
    , WriteBytes_(Context_->Profiler.Counter("/write_bytes"))
    , UpdateRows_(Context_->Profiler.Counter("/update_rows"))
    , EraseRows_(Context_->Profiler.Counter("/erase_rows"))
    , SelectTime_(Context_->Profiler.Timer("/select_time"))
{ }

void TKeyVisitorStates::Reconfigure(TDynamicTableRequestSpecPtr dynamicSpec)
{
    DynamicSpec_ = std::move(dynamicSpec);
}

void TKeyVisitorStates::Write(
    NApi::IDynamicTableTransactionPtr transaction,
    const std::vector<std::pair<TTableKey, std::optional<TValue>>>& mutations)
{
    if (mutations.empty()) {
        return;
    }

    auto nameTable = New<TNameTable>();
    const i32 computationIdField = nameTable->GetIdOrRegisterName("computation_id");
    const i32 streamIdField = nameTable->GetIdOrRegisterName("stream_id");
    const i32 keyField = nameTable->GetIdOrRegisterName("key");
    const i32 isLowerField = nameTable->GetIdOrRegisterName("is_lower");
    const i32 stateField = nameTable->GetIdOrRegisterName("state");

    i64 writeRowCount = 0;
    i64 deleteRowCount = 0;
    auto rowBuffer = New<TRowBuffer>();
    std::vector<TRowModification> rows;
    rows.reserve(mutations.size());
    for (const auto& [tableKey, maybeValue] : mutations) {
        // Sentinels are not storable as `any` sort-keys. The paired row at the
        // opposite boundary carries the full interval in its yson value, so
        // open-ended intervals reconstruct from a single persisted row.
        if (tableKey.Key == MinKey() || tableKey.Key == MaxKey()) {
            continue;
        }
        TUnversionedRowBuilder builder;
        const auto computationId = ToString(tableKey.ComputationId);
        const auto streamId = ToString(tableKey.StreamId);
        const auto keyYsonString = ConvertToYsonString(tableKey.Key.Underlying());
        builder.AddValue(MakeUnversionedStringValue(computationId, computationIdField));
        builder.AddValue(MakeUnversionedStringValue(streamId, streamIdField));
        builder.AddValue(MakeUnversionedAnyValue(keyYsonString.AsStringBuf(), keyField));
        builder.AddValue(MakeUnversionedBooleanValue(tableKey.IsLower, isLowerField));
        if (maybeValue) {
            builder.AddValue(MakeUnversionedAnyValue(maybeValue->AsStringBuf(), stateField));
            auto row = rowBuffer->CaptureRow(builder.GetRow(), /*captureValues*/ true);
            rows.push_back(NRowModifications::TWriteRow(row));
            ++writeRowCount;
        } else {
            auto row = rowBuffer->CaptureRow(builder.GetRow(), /*captureValues*/ true);
            rows.push_back(NRowModifications::TDeleteRow(row));
            ++deleteRowCount;
        }
    }
    auto writeBytes = rowBuffer->GetSize();
    transaction->ModifyRows(TablePath_, nameTable, MakeSharedRange(std::move(rows), std::move(rowBuffer)));

    WriteRows_.Increment(writeRowCount + deleteRowCount);
    WriteBytes_.Increment(writeBytes);
    UpdateRows_.Increment(writeRowCount);
    EraseRows_.Increment(deleteRowCount);
}

TFuture<TKeyVisitorStates::TReadResult> TKeyVisitorStates::Read(
    TTableKeyFilter filter,
    i64 limit,
    std::optional<TTableKey> offsetExclusive)
{
    return BIND([this, strongThis = MakeStrong(this), filter, limit, offsetExclusive] {
        std::vector<std::string> conditions;
        if (filter.ComputationId) {
            conditions.push_back(Format("computation_id = %Qv", *filter.ComputationId));
        }
        if (filter.StreamId) {
            conditions.push_back(Format("stream_id = %Qv", *filter.StreamId));
        }
        // Filter on the composite (key, is_lower) so that scanning a half-open
        // partition range [A; B) returns rows that bracket exactly [A; B):
        // include (A, is_lower=true) but not (A, is_lower=false) — the latter
        // is the upper-bound row of the previous partition; symmetrically
        // include (B, is_lower=false) but not (B, is_lower=true). Plain
        // `key BETWEEN A AND B` would load the neighbour's edge rows and
        // turn into a cross-partition DELETE/WRITE race on Sync.
        if (filter.LowerKey && *filter.LowerKey != MinKey()) {
            conditions.push_back(Format("(key, is_lower) >= (yson_string_to_any(%Qv), true)",
                NYson::ConvertToYsonString(*filter.LowerKey, EYsonFormat::Text)));
        }
        if (filter.UpperKey && *filter.UpperKey != MaxKey()) {
            conditions.push_back(Format("(key, is_lower) <= (yson_string_to_any(%Qv), false)",
                NYson::ConvertToYsonString(*filter.UpperKey, EYsonFormat::Text)));
        }
        if (offsetExclusive) {
            conditions.push_back(Format("(computation_id, stream_id, key, is_lower) > (%Qv, %Qv, yson_string_to_any(%Qv), %v)",
                offsetExclusive->ComputationId,
                offsetExclusive->StreamId,
                NYson::ConvertToYsonString(offsetExclusive->Key, EYsonFormat::Text),
                offsetExclusive->IsLower ? "true" : "false"));
        }

        auto query = Format("computation_id, stream_id, key, is_lower, state from [%v] %v limit %v",
            TablePath_,
            conditions.empty() ? "" : std::string("where ") + JoinSeq(" and ", conditions),
            limit);
        TSelectRowsOptions selectRowsOptions;
        selectRowsOptions.Timestamp = NTransactionClient::SyncLastCommittedTimestamp;
        NProfiling::TWallTimer selectTimer;
        auto selectResult = WaitFor(Context_->Client->SelectRows(query, selectRowsOptions)).ValueOrThrow();
        auto selectElapsed = selectTimer.GetElapsedTime();
        const auto rowset = selectResult.Rowset;

        TReadResult result;
        i64 totalSelectRows = 0;
        i64 totalSelectBytes = 0;
        const auto schema = rowset->GetSchema();
        const i32 computationIdField = schema->GetColumnIndexOrThrow("computation_id");
        const i32 streamIdField = schema->GetColumnIndexOrThrow("stream_id");
        const i32 keyField = schema->GetColumnIndexOrThrow("key");
        const i32 isLowerField = schema->GetColumnIndexOrThrow("is_lower");
        const i32 stateField = schema->GetColumnIndexOrThrow("state");
        for (const auto& row : rowset->GetRows()) {
            if (!row) {
                continue;
            }
            TTableKey tableKey;
            tableKey.ComputationId = FromUnversionedValue<TComputationId>(row[computationIdField]);
            tableKey.StreamId = FromUnversionedValue<TStreamId>(row[streamIdField]);
            tableKey.Key = ConvertTo<TKey>(FromUnversionedValue<TYsonString>(row[keyField]));
            tableKey.IsLower = FromUnversionedValue<bool>(row[isLowerField]);
            auto value = FromUnversionedValue<TYsonString>(row[stateField]);
            result.Rows.push_back({std::move(tableKey), std::move(value)});
            auto weight = GetDataWeight(row);
            ++totalSelectRows;
            totalSelectBytes += weight;
        }
        if (std::ssize(rowset->GetRows()) == limit && !result.Rows.empty()) {
            result.ContinuationOffsetExclusive = result.Rows.back().first;
        }
        SelectRows_.Increment(totalSelectRows);
        SelectBytes_.Increment(totalSelectBytes);
        SelectTime_.Record(selectElapsed);
        return result;
    })
        .AsyncVia(GetCurrentInvoker())
        .Run();
}

TFuture<std::vector<std::pair<TKeyVisitorStates::TTableKey, TKeyVisitorStates::TValue>>>
TKeyVisitorStates::ReadAll(TTableKeyFilter filter)
{
    return BIND([this, strongThis = MakeStrong(this), filter] {
        std::vector<std::pair<TTableKey, TValue>> rows;
        std::optional<TTableKey> offsetExclusive;
        TSelectLimiter limiter(DynamicSpec_);
        while (true) {
            auto result = WaitFor(Read(filter, limiter.Get(), offsetExclusive)).ValueOrThrow();
            rows.insert(rows.end(),
                std::make_move_iterator(result.Rows.begin()),
                std::make_move_iterator(result.Rows.end()));
            if (!result.ContinuationOffsetExclusive) {
                break;
            }
            offsetExclusive = result.ContinuationOffsetExclusive;
        }
        return rows;
    })
        .AsyncVia(GetCurrentInvoker())
        .Run();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NTables

size_t THash<NYT::NFlow::NTables::IKeyVisitorStates::TTableKey>::operator()(
    const NYT::NFlow::NTables::IKeyVisitorStates::TTableKey& tableKey) const
{
    auto value = std::tuple(tableKey.ComputationId, tableKey.StreamId, tableKey.Key, tableKey.IsLower);
    return THash<decltype(value)>()(value);
}
