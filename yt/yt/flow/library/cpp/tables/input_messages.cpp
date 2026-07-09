#include "input_messages.h"

#include "common.h"
#include "context.h"

#include <yt/yt/flow/library/cpp/common/spec.h>

#include <yt/yt/flow/library/cpp/misc/load_throughput_throttler.h>

#include <yt/yt/flow/lib/native_client/public.h>

#include <yt/yt/client/api/client.h>
#include <yt/yt/client/api/dynamic_table_client.h>
#include <yt/yt/client/api/transaction.h>

#include <yt/yt/client/table_client/helpers.h>
#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/row_buffer.h>
#include <yt/yt/client/table_client/unversioned_row.h>

#include <yt/yt/core/profiling/timing.h>

#include <yt/yt/core/ypath/helpers.h>

#include <util/digest/city.h>
#include <util/string/join.h>

namespace NYT::NFlow::NTables {

using namespace NApi;
using namespace NConcurrency;
using namespace NTableClient;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

TInputMessages::TInputMessages(TContextPtr context)
    : Context_(context->WithTableName(InputMessagesTableName))
    , TablePath_(NYPath::YPathJoin(Context_->PipelinePath.GetPath(), InputMessagesTableName))
    , Logger(Context_->Logger)
    , Tag_(Context_->Tag.value_or(std::string{InputMessagesTableName}))
    , Metrics_{.Profiler = Context_->Profiler.WithTag("tag", Tag_)}
{ }

TFuture<std::vector<bool>> TInputMessages::Contains(
    const TComputationId& computationId,
    const std::vector<TMessage>& messages)
{
    if (messages.empty()) {
        return MakeFuture(std::vector<bool>{});
    }

    auto nameTable = New<TNameTable>();
    const i32 computationIdField = nameTable->GetIdOrRegisterName("computation_id");
    const i32 keyField = nameTable->GetIdOrRegisterName("key");
    const i32 messageIdField = nameTable->GetIdOrRegisterName("message_id");
    const i32 systemTimestampField = nameTable->GetIdOrRegisterName("system_timestamp");

    TLookupRowsOptions lookupOptions;
    lookupOptions.ColumnFilter = TColumnFilter({keyField, messageIdField, systemTimestampField});
    lookupOptions.KeepMissingRows = true;
    lookupOptions.Timestamp = NTransactionClient::SyncLastCommittedTimestamp;

    auto rowBuffer = New<TRowBuffer>();
    std::vector<TUnversionedRow> rows;
    rows.reserve(messages.size());

    for (const auto& message : messages) {
        TUnversionedRowBuilder builder;
        builder.AddValue(MakeUnversionedStringValue(computationId.Underlying(), computationIdField));
        auto keyYsonString = ConvertToYsonString(message.Key.Underlying());
        builder.AddValue(MakeUnversionedAnyValue(keyYsonString.AsStringBuf(), keyField));
        builder.AddValue(MakeUnversionedStringValue(message.MessageId.Underlying(), messageIdField));
        rows.push_back(rowBuffer->CaptureRow(builder.GetRow()));
    }

    const auto range = MakeSharedRange(std::move(rows), std::move(rowBuffer));

    return BIND([this, strongThis = MakeStrong(this), range, nameTable, lookupOptions, expectedSize = std::ssize(messages)] () -> std::vector<bool> {
        WaitFor(Context_->LoadThroughputThrottler->ThrottleRows(Tag_, expectedSize)).ThrowOnError();

        NProfiling::TWallTimer lookupTimer;
        auto rowset = WaitFor(Context_->Client->LookupRows(TablePath_, nameTable, range, lookupOptions)).ValueOrThrow().Rowset;
        i64 totalBytes = 0;
        std::vector<bool> result;
        for (const auto& row : rowset->GetRows()) {
            if (row) {
                result.push_back(true);
                totalBytes += GetDataWeight(row);
            } else {
                result.push_back(false);
            }
        }
        YT_VERIFY(std::ssize(result) == expectedSize);
        Metrics_.LookupRows.Increment(std::ssize(result));
        Metrics_.LookupBytes.Increment(totalBytes);
        Metrics_.LookupTime.Record(lookupTimer.GetElapsedTime());
        return result;
    })
        .AsyncVia(GetCurrentInvoker())
        .Run();
}

void TInputMessages::Write(
    IDynamicTableTransactionPtr transaction,
    const TComputationId& computationId,
    const std::vector<TMessage>& messages)
{
    if (messages.empty()) {
        return;
    }

    auto nameTable = New<TNameTable>();
    const i32 computationIdField = nameTable->GetIdOrRegisterName("computation_id");
    const i32 keyField = nameTable->GetIdOrRegisterName("key");
    const i32 messageIdField = nameTable->GetIdOrRegisterName("message_id");
    const i32 systemTimestampField = nameTable->GetIdOrRegisterName("system_timestamp");

    auto rowBuffer = New<TRowBuffer>();
    std::vector<TRowModification> rows;
    rows.reserve(messages.size());
    i64 totalBytes = 0;
    for (const auto& message : messages) {
        TUnversionedRowBuilder builder;
        builder.AddValue(MakeUnversionedStringValue(computationId.Underlying(), computationIdField));
        auto keyYsonString = ConvertToYsonString(message.Key.Underlying());
        builder.AddValue(MakeUnversionedAnyValue(keyYsonString.AsStringBuf(), keyField));
        builder.AddValue(MakeUnversionedStringValue(message.MessageId.Underlying(), messageIdField));
        builder.AddValue(MakeUnversionedUint64Value(message.SystemTimestamp.Underlying(), systemTimestampField));
        auto row = rowBuffer->CaptureRow(builder.GetRow());
        totalBytes += GetDataWeight(row);
        rows.push_back(NRowModifications::TWriteRow(row));
    }
    transaction->ModifyRows(TablePath_, nameTable, MakeSharedRange(std::move(rows), std::move(rowBuffer)));

    Metrics_.WriteRows.Increment(std::ssize(messages));
    Metrics_.WriteBytes.Increment(totalBytes);
}

////////////////////////////////////////////////////////////////////////////////

TCompactInputMessages::TCompactInputMessages(TContextPtr context)
    : Context_(context->WithTableName(CompactInputMessagesTableName))
    , TablePath_(NYPath::YPathJoin(Context_->PipelinePath.GetPath(), CompactInputMessagesTableName))
    , Logger(Context_->Logger)
    , Tag_(Context_->Tag.value_or(std::string{CompactInputMessagesTableName}))
    , Metrics_{.Profiler = Context_->Profiler.WithTag("tag", Tag_)}
{ }

namespace {

void AppendUint64BigEndian(TString& buffer, ui64 value)
{
    for (int byteIndex = 7; byteIndex >= 0; --byteIndex) {
        buffer.push_back(static_cast<char>((value >> (8 * byteIndex)) & 0xff));
    }
}

} // namespace

void TCompactInputMessages::BuildDeduplicationKey(
    const TComputationId& computationId,
    const TMessage& message,
    TString& buffer)
{
    const auto& key = message.Key.Underlying();
    const ui64 keyHash = key.GetCount() > 0
        ? FromUnversionedValue<ui64>(key[0])
        : 0;
    const auto messageIdHash = CityHash128(
        message.MessageId.Underlying().data(),
        message.MessageId.Underlying().size());

    buffer.clear();
    buffer.reserve(computationId.Underlying().size() + 1 + 8 + 16);
    buffer.append(computationId.Underlying());
    buffer.push_back('\0');
    AppendUint64BigEndian(buffer, keyHash);
    AppendUint64BigEndian(buffer, messageIdHash.first);
    AppendUint64BigEndian(buffer, messageIdHash.second);
}

TFuture<std::vector<bool>> TCompactInputMessages::Contains(
    const TComputationId& computationId,
    const std::vector<TMessage>& messages)
{
    if (messages.empty()) {
        return MakeFuture(std::vector<bool>{});
    }

    auto nameTable = New<TNameTable>();
    const i32 deduplicationKeyField = nameTable->GetIdOrRegisterName("deduplication_message_key");
    const i32 systemTimestampField = nameTable->GetIdOrRegisterName("system_timestamp");

    TLookupRowsOptions lookupOptions;
    lookupOptions.ColumnFilter = TColumnFilter({systemTimestampField});
    lookupOptions.KeepMissingRows = true;
    lookupOptions.Timestamp = NTransactionClient::SyncLastCommittedTimestamp;

    auto rowBuffer = New<TRowBuffer>();
    std::vector<TUnversionedRow> rows;
    rows.reserve(messages.size());

    TString keyBuffer;
    for (const auto& message : messages) {
        TUnversionedRowBuilder builder;
        BuildDeduplicationKey(computationId, message, keyBuffer);
        builder.AddValue(MakeUnversionedStringValue(TStringBuf(keyBuffer), deduplicationKeyField));
        rows.push_back(rowBuffer->CaptureRow(builder.GetRow()));
    }

    const auto range = MakeSharedRange(std::move(rows), std::move(rowBuffer));

    return BIND([this, strongThis = MakeStrong(this), range, nameTable, lookupOptions, expectedSize = std::ssize(messages)] () -> std::vector<bool> {
        WaitFor(Context_->LoadThroughputThrottler->ThrottleRows(Tag_, expectedSize)).ThrowOnError();

        NProfiling::TWallTimer lookupTimer;
        auto rowset = WaitFor(Context_->Client->LookupRows(TablePath_, nameTable, range, lookupOptions)).ValueOrThrow().Rowset;
        i64 totalBytes = 0;
        std::vector<bool> result;
        for (const auto& row : rowset->GetRows()) {
            if (row) {
                result.push_back(true);
                totalBytes += GetDataWeight(row);
            } else {
                result.push_back(false);
            }
        }
        YT_VERIFY(std::ssize(result) == expectedSize);
        Metrics_.LookupRows.Increment(std::ssize(result));
        Metrics_.LookupBytes.Increment(totalBytes);
        Metrics_.LookupTime.Record(lookupTimer.GetElapsedTime());
        return result;
    })
        .AsyncVia(GetCurrentInvoker())
        .Run();
}

void TCompactInputMessages::Write(
    IDynamicTableTransactionPtr transaction,
    const TComputationId& computationId,
    const std::vector<TMessage>& messages)
{
    if (messages.empty()) {
        return;
    }

    auto nameTable = New<TNameTable>();
    const i32 deduplicationKeyField = nameTable->GetIdOrRegisterName("deduplication_message_key");
    const i32 systemTimestampField = nameTable->GetIdOrRegisterName("system_timestamp");

    auto rowBuffer = New<TRowBuffer>();
    std::vector<TRowModification> rows;
    rows.reserve(messages.size());
    i64 totalBytes = 0;
    TString keyBuffer;
    for (const auto& message : messages) {
        TUnversionedRowBuilder builder;
        BuildDeduplicationKey(computationId, message, keyBuffer);
        builder.AddValue(MakeUnversionedStringValue(TStringBuf(keyBuffer), deduplicationKeyField));
        builder.AddValue(MakeUnversionedUint64Value(message.SystemTimestamp.Underlying(), systemTimestampField));
        auto row = rowBuffer->CaptureRow(builder.GetRow());
        totalBytes += GetDataWeight(row);
        rows.push_back(NRowModifications::TWriteRow(row));
    }
    transaction->ModifyRows(TablePath_, nameTable, MakeSharedRange(std::move(rows), std::move(rowBuffer)));

    Metrics_.WriteRows.Increment(std::ssize(messages));
    Metrics_.WriteBytes.Increment(totalBytes);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NTables
