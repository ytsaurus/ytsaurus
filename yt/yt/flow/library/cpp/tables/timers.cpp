#include "timers.h"

#include "common.h"
#include "context.h"

#include <yt/yt/flow/library/cpp/common/spec.h>

#include <yt/yt/flow/library/cpp/misc/load_throughput_throttler.h>

#include <yt/yt/client/api/client.h>
#include <yt/yt/client/api/dynamic_table_client.h>
#include <yt/yt/client/api/transaction.h>

#include <yt/yt/client/table_client/helpers.h>
#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/row_buffer.h>
#include <yt/yt/client/table_client/unversioned_row.h>

#include <yt/yt/core/profiling/timing.h>

#include <yt/yt/core/ypath/helpers.h>

#include <util/string/join.h>

namespace NYT::NFlow::NTables {

using namespace NApi;
using namespace NConcurrency;
using namespace NTableClient;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

TTimers::TTimers(
    TContextPtr context,
    TDynamicTableRequestSpecPtr dynamicSpec)
    : Context_(context->WithTableName(TimersTableName))
    , DynamicSpec_(std::move(dynamicSpec))
    , TablePath_(NYPath::YPathJoin(Context_->PipelinePath.GetPath(), TimersTableName))
    , Logger(Context_->Logger)
    , Tag_(Context_->Tag.value_or(std::string{TimersTableName}))
    , Metrics_{.Profiler = Context_->Profiler.WithTag("tag", Tag_)}
{ }

void TTimers::Reconfigure(TDynamicTableRequestSpecPtr dynamicSpec)
{
    DynamicSpec_ = std::move(dynamicSpec);
}

TFuture<TTimers::TLoadResult> TTimers::Load(
    TFilter filter,
    i64 limit,
    std::optional<TTableKey> offsetExclusive)
{
    return BIND([this, strongThis = MakeStrong(this), filter, limit, offsetExclusive] {
        WaitFor(Context_->LoadThroughputThrottler->ThrottleKeys(Tag_, limit)).ThrowOnError();

        std::vector<std::string> conditions;
        if (filter.ComputationId) {
            conditions.push_back(Format("computation_id = %Qv", *filter.ComputationId));
        }
        if (filter.ExactKey) {
            conditions.push_back(Format("key = yson_string_to_any(%Qv)",
                NYson::ConvertToYsonString(*filter.ExactKey, EYsonFormat::Text)));
        }
        if (filter.LowerKey && *filter.LowerKey != MinKey()) {
            conditions.push_back(Format("key >= yson_string_to_any(%Qv)",
                NYson::ConvertToYsonString(*filter.LowerKey, EYsonFormat::Text)));
        }
        if (filter.UpperKey && *filter.UpperKey != MaxKey()) {
            conditions.push_back(Format("key < yson_string_to_any(%Qv)",
                NYson::ConvertToYsonString(*filter.UpperKey, EYsonFormat::Text)));
        }
        if (offsetExclusive) {
            conditions.push_back(Format("(computation_id, key, message_id) > (%Qv, yson_string_to_any(%Qv), %Qv)",
                offsetExclusive->ComputationId,
                NYson::ConvertToYsonString(offsetExclusive->Key, EYsonFormat::Text),
                offsetExclusive->MessageId));
        }

        auto query = Format("computation_id, key, message_id, stream_id, system_timestamp, event_timestamp, trigger_timestamp from [%v] %v limit %v",
            TablePath_,
            conditions.empty() ? "" : std::string("where ") + JoinSeq(" and ", conditions),
            limit);
        TSelectRowsOptions selectRowsOptions;
        selectRowsOptions.Timestamp = NTransactionClient::SyncLastCommittedTimestamp;

        NProfiling::TWallTimer loadTimer;
        auto selectResult = WaitFor(Context_->Client->SelectRows(query, selectRowsOptions)).ValueOrThrow();
        auto selectElapsed = loadTimer.GetElapsedTime();
        const auto rowset = selectResult.Rowset;

        TLoadResult result;
        result.Timers.reserve(limit);
        {
            std::vector<i64> sizes;
            const auto schema = rowset->GetSchema();
            const i32 computationIdField = schema->GetColumnIndexOrThrow("computation_id");
            const i32 keyField = schema->GetColumnIndexOrThrow("key");
            const i32 messageIdField = schema->GetColumnIndexOrThrow("message_id");
            const i32 streamIdField = schema->GetColumnIndexOrThrow("stream_id");
            const i32 systemTimestampField = schema->GetColumnIndexOrThrow("system_timestamp");
            const i32 eventTimestampField = schema->GetColumnIndexOrThrow("event_timestamp");
            const i32 triggerTimestampField = schema->GetColumnIndexOrThrow("trigger_timestamp");

            for (const auto& row : rowset->GetRows()) {
                if (row) {
                    const auto computationId = FromUnversionedValue<TComputationId>(row[computationIdField]);
                    TTimer timer;
                    timer.MessageId = FromUnversionedValue<TMessageId>(row[messageIdField]);
                    timer.SystemTimestamp = FromUnversionedValue<TSystemTimestamp>(row[systemTimestampField]);
                    timer.AlignmentTimestamp = timer.SystemTimestamp; // The alignment timestamp is the persist timestamp.
                    timer.EventTimestamp = FromUnversionedValue<TSystemTimestamp>(row[eventTimestampField]);
                    timer.StreamId = FromUnversionedValue<TStreamId>(row[streamIdField]);
                    timer.Key = ConvertTo<TKey>(FromUnversionedValue<TYsonString>(row[keyField]));
                    timer.TriggerTimestamp = FromUnversionedValue<TSystemTimestamp>(row[triggerTimestampField]);

                    TTableKey tableKey{
                        .ComputationId = computationId,
                        .Key = timer.Key,
                        .MessageId = timer.MessageId,
                    };
                    result.Timers.push_back({tableKey, timer});
                    sizes.push_back(GetDataWeight(row));
                }
            }
            Context_->LoadThroughputThrottler->RegisterKeys(Tag_, sizes);

            i64 totalBytes = 0;
            for (auto s : sizes) {
                totalBytes += s;
            }
            Metrics_.SelectRows.Increment(std::ssize(result.Timers));
            Metrics_.SelectBytes.Increment(totalBytes);
        }
        Metrics_.SelectTime.Record(selectElapsed);

        if (std::ssize(rowset->GetRows()) == limit) {
            result.ContinuationOffsetExclusive = result.Timers.back().first;
        }
        return result;
    })
        .AsyncVia(GetCurrentInvoker())
        .Run();
}

TFuture<std::vector<std::pair<TTimers::TTableKey, TTimer>>> TTimers::LoadAll(
    TFilter filter)
{
    return BIND([this, strongThis = MakeStrong(this), filter] () {
        std::vector<std::pair<TTableKey, TTimer>> timers;
        std::optional<TTableKey> offsetExclusive;
        TSelectLimiter limiter(DynamicSpec_);
        while (true) {
            auto result = WaitFor(Load(filter, limiter.Get(), offsetExclusive)).ValueOrThrow();
            timers.insert(timers.end(), result.Timers.begin(), result.Timers.end());
            if (!result.ContinuationOffsetExclusive) {
                break;
            }
            offsetExclusive = result.ContinuationOffsetExclusive;
        }
        return timers;
    })
        .AsyncVia(GetCurrentInvoker())
        .Run();
}

void TTimers::Write(
    NApi::IDynamicTableTransactionPtr transaction,
    const TComputationId& computationId,
    const std::vector<TTimer>& timers)
{
    if (timers.empty()) {
        return;
    }

    auto nameTable = New<TNameTable>();
    const i32 computationIdField = nameTable->GetIdOrRegisterName("computation_id");
    const i32 keyField = nameTable->GetIdOrRegisterName("key");
    const i32 messageIdField = nameTable->GetIdOrRegisterName("message_id");
    const i32 streamIdField = nameTable->GetIdOrRegisterName("stream_id");
    const i32 systemTimestampField = nameTable->GetIdOrRegisterName("system_timestamp");
    const i32 eventTimestampField = nameTable->GetIdOrRegisterName("event_timestamp");
    const i32 triggerTimestampField = nameTable->GetIdOrRegisterName("trigger_timestamp");

    auto rowBuffer = New<TRowBuffer>();
    std::vector<TRowModification> rows;
    rows.reserve(timers.size());
    i64 totalBytes = 0;
    for (const auto& timer : timers) {
        TUnversionedRowBuilder builder;
        const auto keyYsonString = ConvertToYsonString(timer.Key.Underlying());
        builder.AddValue(MakeUnversionedStringValue(computationId.Underlying(), computationIdField));
        builder.AddValue(MakeUnversionedAnyValue(keyYsonString.AsStringBuf(), keyField));
        builder.AddValue(MakeUnversionedStringValue(timer.MessageId.Underlying(), messageIdField));
        builder.AddValue(MakeUnversionedStringValue(timer.StreamId.Underlying(), streamIdField));
        builder.AddValue(MakeUnversionedUint64Value(timer.SystemTimestamp.Underlying(), systemTimestampField));
        builder.AddValue(MakeUnversionedUint64Value(timer.EventTimestamp.Underlying(), eventTimestampField));
        builder.AddValue(MakeUnversionedUint64Value(timer.TriggerTimestamp.Underlying(), triggerTimestampField));
        auto row = rowBuffer->CaptureRow(builder.GetRow(), /*captureValues*/ true);
        totalBytes += GetDataWeight(row);
        rows.push_back(NRowModifications::TWriteRow(row));
    }
    transaction->ModifyRows(TablePath_, nameTable, MakeSharedRange(std::move(rows), std::move(rowBuffer)));

    Metrics_.WriteRows.Increment(std::ssize(timers));
    Metrics_.WriteBytes.Increment(totalBytes);
}

void TTimers::Erase(
    NApi::IDynamicTableTransactionPtr transaction,
    const std::vector<TTableKey>& tableKeys)
{
    if (tableKeys.empty()) {
        return;
    }

    auto nameTable = New<TNameTable>();
    const i32 computationIdField = nameTable->GetIdOrRegisterName("computation_id");
    const i32 keyField = nameTable->GetIdOrRegisterName("key");
    const i32 messageIdField = nameTable->GetIdOrRegisterName("message_id");

    auto rowBuffer = New<TRowBuffer>();
    std::vector<TRowModification> rows;
    rows.reserve(tableKeys.size());
    for (const auto& tableKey : tableKeys) {
        TUnversionedRowBuilder builder;
        const auto keyYsonString = ConvertToYsonString(tableKey.Key.Underlying());
        builder.AddValue(MakeUnversionedStringValue(tableKey.ComputationId.Underlying(), computationIdField));
        builder.AddValue(MakeUnversionedAnyValue(keyYsonString.AsStringBuf(), keyField));
        builder.AddValue(MakeUnversionedStringValue(tableKey.MessageId.Underlying(), messageIdField));
        auto row = rowBuffer->CaptureRow(builder.GetRow(), /*captureValues*/ true);
        rows.push_back(NRowModifications::TDeleteRow(row));
    }
    transaction->ModifyRows(TablePath_, nameTable, MakeSharedRange(std::move(rows), std::move(rowBuffer)));

    Metrics_.EraseRows.Increment(std::ssize(tableKeys));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NTables
