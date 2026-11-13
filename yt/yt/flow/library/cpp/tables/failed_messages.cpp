#include "failed_messages.h"

#include "context.h"

#include <yt/yt/flow/library/cpp/common/stream_spec_storage.h>

#include <yt/yt/flow/library/cpp/native_client/public.h>

#include <yt/yt/flow/library/cpp/misc/load_throughput_throttler.h>
#include <yt/yt/flow/library/cpp/misc/retryable_transaction.h>

#include <yt/yt/client/api/transaction.h>

#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/row_buffer.h>
#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/unversioned_row.h>

#include <yt/yt/core/misc/protobuf_helpers.h>

#include <yt/yt/core/ypath/helpers.h>

#include <yt/yt/core/ytree/fluent.h>

#include <util/datetime/base.h>

#include <util/generic/hash_set.h>

#include <tuple>

namespace NYT::NFlow::NTables {

using namespace NApi;
using namespace NConcurrency;
using namespace NTableClient;
using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

TFailedMessages::TFailedMessages(
    TContextPtr context,
    TStreamSpecsPtr streamSpecs,
    std::string workerId,
    TFlowCoreTarget flowCoreTarget)
    : Context_(context->WithTableName(FailedMessagesTableName))
    , StreamSpecs_(std::move(streamSpecs))
    , WorkerId_(std::move(workerId))
    , FlowCoreTarget_(std::move(flowCoreTarget))
    , TablePath_(NYPath::YPathJoin(Context_->PipelinePath.GetPath(), FailedMessagesTableName))
    , CapturedRows_(Context_->Profiler
            .WithTag("tag", Context_->Tag.value_or(std::string(FailedMessagesTableName)))
            .Counter("/captured_rows"))
{ }

void TFailedMessages::Write(
    const IRetryableTransactionPtr& transaction,
    const TComputationId& computationId,
    const TPartitionId& partitionId,
    TSystemTimestamp systemTimestamp,
    const std::vector<TFailedMessage>& messages)
{
    if (messages.empty()) {
        return;
    }

    auto nameTable = New<TNameTable>();
    const auto computationIdField = nameTable->GetIdOrRegisterName("computation_id");
    const auto streamIdField = nameTable->GetIdOrRegisterName("stream_id");
    const auto keyField = nameTable->GetIdOrRegisterName("key");
    const auto messageIdField = nameTable->GetIdOrRegisterName("message_id");
    const auto eventTimestampField = nameTable->GetIdOrRegisterName("event_timestamp");
    const auto payloadField = nameTable->GetIdOrRegisterName("payload");
    const auto streamSpecIdField = nameTable->GetIdOrRegisterName("stream_spec_id");
    const auto failedAtField = nameTable->GetIdOrRegisterName("failed_at");
    const auto replayAttemptsField = nameTable->GetIdOrRegisterName("replay_attempts");
    const auto diagnosticField = nameTable->GetIdOrRegisterName("diagnostic");

    auto rowBuffer = New<TRowBuffer>();
    std::vector<TUnversionedRow> keys;
    std::vector<TUnversionedRow> rows;
    THashSet<std::tuple<TStreamId, TKey, TMessageId>> seen;
    for (const auto& failure : messages) {
        const auto& message = failure.Message;
        if (!seen.emplace(message->StreamId, message->Key, message->MessageId).second) {
            continue;
        }

        TUnversionedRowBuilder builder;
        builder.AddValue(MakeUnversionedStringValue(computationId.Underlying(), computationIdField));
        builder.AddValue(MakeUnversionedStringValue(message->StreamId.Underlying(), streamIdField));
        auto key = ConvertToYsonString(message->Key.Underlying());
        builder.AddValue(MakeUnversionedAnyValue(key.AsStringBuf(), keyField));
        builder.AddValue(MakeUnversionedStringValue(message->MessageId.Underlying(), messageIdField));
        keys.push_back(rowBuffer->CaptureRow(builder.GetRow()));

        auto payload = ToProto<TProtobufString>(message->Payload);
        auto streamSpecId = StreamSpecs_->GetStreamSpecId(message->PayloadSchema);
        // clang-format off
        auto diagnostic = BuildYsonStringFluently()
            .BeginMap()
                .Item("partition_id").Value(partitionId.Underlying())
                .Item("system_timestamp").Value(message->SystemTimestamp.Underlying())
                .Item("flow_core_target").Value(FlowCoreTarget_.Underlying())
                .Item("worker_id").Value(WorkerId_)
                .Item("error").Value(failure.Reason)
            .EndMap();
        // clang-format on
        builder.AddValue(MakeUnversionedUint64Value(message->EventTimestamp.Underlying(), eventTimestampField));
        builder.AddValue(MakeUnversionedStringValue(payload, payloadField));
        builder.AddValue(MakeUnversionedInt64Value(streamSpecId.Underlying(), streamSpecIdField));
        builder.AddValue(MakeUnversionedUint64Value(TInstant::Seconds(systemTimestamp.Underlying()).MilliSeconds(), failedAtField));
        builder.AddValue(MakeUnversionedUint64Value(0, replayAttemptsField));
        builder.AddValue(MakeUnversionedAnyValue(diagnostic.AsStringBuf(), diagnosticField));
        rows.push_back(rowBuffer->CaptureRow(builder.GetRow()));
    }

    CapturedRows_.Increment(std::ssize(rows));

    // Check every attempt to keep a repeated capture from overwriting an existing failure.
    transaction->Apply(BIND([
        path = TablePath_,
        nameTable,
        rowBuffer,
        computationIdField,
        throttler = Context_->LoadThroughputThrottler,
        tag = Context_->Tag.value_or(std::string(FailedMessagesTableName)),
        keys = MakeSharedRange(std::move(keys), rowBuffer),
        rows = MakeSharedRange(std::move(rows), rowBuffer)
    ] (const ITransactionPtr& attempt) {
        WaitFor(throttler->ThrottleRows(tag, keys.Size()))
            .ThrowOnError();
        TLookupRowsOptions options;
        options.KeepMissingRows = true;
        options.ColumnFilter = TColumnFilter({computationIdField});
        options.Timestamp = attempt->GetStartTimestamp();
        auto existing = WaitFor(attempt->LookupRows(path, nameTable, keys, options))
            .ValueOrThrow()
            .Rowset;
        YT_VERIFY(existing->GetRows().Size() == rows.Size());

        std::vector<TRowModification> modifications;
        for (i64 index = 0; index < static_cast<i64>(rows.Size()); ++index) {
            if (!existing->GetRows()[index]) {
                modifications.push_back(NRowModifications::TWriteRow(rows[index]));
            }
        }
        if (!modifications.empty()) {
            attempt->ModifyRows(path, nameTable, MakeSharedRange(std::move(modifications), rowBuffer));
        }
    }));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NTables
