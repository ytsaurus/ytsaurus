#include "compact_output_messages.h"

#include "common.h"
#include "context.h"

#include <yt/yt/flow/library/cpp/common/key.h>

#include <yt/yt/flow/library/cpp/misc/load_throughput_throttler.h>

#include <yt/yt/client/api/client.h>
#include <yt/yt/client/api/dynamic_table_client.h>
#include <yt/yt/client/api/transaction.h>

#include <yt/yt/client/table_client/helpers.h>
#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/row_buffer.h>
#include <yt/yt/client/table_client/unversioned_row.h>

#include <yt/yt/core/compression/codec.h>

#include <yt/yt/core/misc/protobuf_helpers.h>

#include <yt/yt/core/profiling/timing.h>

#include <yt/yt/core/ypath/helpers.h>

#include <util/string/join.h>

namespace NYT::NFlow::NTables {

using namespace NApi;
using namespace NConcurrency;
using namespace NTableClient;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

namespace {

class TCompactOutputMessages
    : public ICompactOutputMessages
{
public:
    TCompactOutputMessages(TContextPtr context, TDynamicTableRequestSpecPtr dynamicSpec)
        : Context_(context->WithTableName(CompactOutputMessagesTableName))
        , DynamicSpec_(std::move(dynamicSpec))
        , TablePath_(NYPath::YPathJoin(Context_->PipelinePath.GetPath(), CompactOutputMessagesTableName))
        , Logger(Context_->Logger)
        , Tag_(Context_->Tag.value_or(std::string{CompactOutputMessagesTableName}))
        , Metrics_{.Profiler = Context_->Profiler.WithTag("tag", Tag_)}
    { }

    void Reconfigure(TDynamicTableRequestSpecPtr dynamicSpec) override
    {
        DynamicSpec_ = std::move(dynamicSpec);
    }

    TFuture<std::vector<TChunk>> LoadAll(TFilter filter) override
    {
        return BIND([this, strongThis = MakeStrong(this), filter] {
            std::vector<std::string> conditions;
            if (filter.ComputationId) {
                conditions.push_back(Format("computation_id = %Qv", *filter.ComputationId));
            }
            if (filter.ExactKey) {
                conditions.push_back(Format("key = yson_string_to_any(%Qv)",
                    ConvertToYsonString(*filter.ExactKey, EYsonFormat::Text)));
            }
            if (filter.LowerKey && *filter.LowerKey != MinKey()) {
                conditions.push_back(Format("key >= yson_string_to_any(%Qv)",
                    ConvertToYsonString(*filter.LowerKey, EYsonFormat::Text)));
            }
            if (filter.UpperKey && *filter.UpperKey != MaxKey()) {
                conditions.push_back(Format("key < yson_string_to_any(%Qv)",
                    ConvertToYsonString(*filter.UpperKey, EYsonFormat::Text)));
            }
            auto query = Format("computation_id, key, stream_id, chunk_id, data, data_codec, processed_mask from [%v]%v",
                TablePath_,
                conditions.empty() ? "" : std::string(" where ") + JoinSeq(" and ", conditions));
            TSelectRowsOptions selectRowsOptions;
            selectRowsOptions.Timestamp = NTransactionClient::SyncLastCommittedTimestamp;

            NProfiling::TWallTimer loadTimer;
            auto selectResult = WaitFor(Context_->Client->SelectRows(query, selectRowsOptions)).ValueOrThrow();
            auto selectElapsed = loadTimer.GetElapsedTime();
            const auto& rowset = selectResult.Rowset;

            std::vector<TChunk> chunks;
            std::vector<i64> sizes;
            i64 totalBytes = 0;
            {
                const auto& nameTable = rowset->GetNameTable();
                const i32 computationIdField = nameTable->GetIdOrRegisterName("computation_id");
                const i32 keyField = nameTable->GetIdOrRegisterName("key");
                const i32 streamIdField = nameTable->GetIdOrRegisterName("stream_id");
                const i32 chunkIdField = nameTable->GetIdOrRegisterName("chunk_id");
                const i32 dataField = nameTable->GetIdOrRegisterName("data");
                const i32 dataCodecField = nameTable->GetIdOrRegisterName("data_codec");
                const i32 processedMaskField = nameTable->GetIdOrRegisterName("processed_mask");

                for (const auto& row : rowset->GetRows()) {
                    if (!row) {
                        continue;
                    }

                    auto encodedCodec = FromUnversionedValue<std::optional<NCompression::ECodec>>(row[dataCodecField])
                        .value_or(NCompression::ECodec::None);
                    auto rawData = TSharedRef::FromString(FromUnversionedValue<std::string>(row[dataField]));
                    if (encodedCodec != NCompression::ECodec::None) {
                        rawData = NCompression::GetCodec(encodedCodec)->Decompress(rawData);
                    }

                    auto processedMask = FromUnversionedValue<std::optional<std::string>>(row[processedMaskField])
                        .value_or(std::string{});

                    auto weight = GetDataWeight(row);
                    sizes.push_back(weight);
                    totalBytes += weight;
                    chunks.push_back(TChunk{
                        .Key = TTableKey{
                            .ComputationId = FromUnversionedValue<TComputationId>(row[computationIdField]),
                            .Key = ConvertTo<TKey>(FromUnversionedValue<TYsonString>(row[keyField])),
                            .StreamId = TStreamId(FromUnversionedValue<std::string>(row[streamIdField])),
                            .ChunkId = FromUnversionedValue<i64>(row[chunkIdField]),
                        },
                        .Data = std::move(rawData),
                        .ProcessedMask = std::move(processedMask),
                    });
                }
            }
            Context_->LoadThroughputThrottler->RegisterRows(Tag_, sizes);

            Metrics_.SelectRows.Increment(std::ssize(chunks));
            Metrics_.SelectBytes.Increment(totalBytes);
            Metrics_.SelectTime.Record(selectElapsed);

            return chunks;
        })
            .AsyncVia(GetCurrentInvoker())
            .Run();
    }

    void Write(
        IDynamicTableTransactionPtr transaction,
        const std::vector<TChunk>& chunks,
        NCompression::ECodec codecId) override
    {
        if (chunks.empty()) {
            return;
        }
        auto codec = NCompression::GetCodec(codecId);

        auto nameTable = New<TNameTable>();
        const i32 computationIdField = nameTable->GetIdOrRegisterName("computation_id");
        const i32 keyField = nameTable->GetIdOrRegisterName("key");
        const i32 streamIdField = nameTable->GetIdOrRegisterName("stream_id");
        const i32 chunkIdField = nameTable->GetIdOrRegisterName("chunk_id");
        const i32 dataField = nameTable->GetIdOrRegisterName("data");
        const i32 dataCodecField = nameTable->GetIdOrRegisterName("data_codec");
        const i32 processedMaskField = nameTable->GetIdOrRegisterName("processed_mask");

        auto rowBuffer = New<TRowBuffer>();
        std::vector<TRowModification> rows;
        rows.reserve(chunks.size());
        i64 totalBytes = 0;
        // Keeps compressed buffers alive for the row's TStringBuf views.
        std::vector<TSharedRef> compressedKeepalive;
        compressedKeepalive.reserve(chunks.size());
        for (const auto& chunk : chunks) {
            compressedKeepalive.push_back(codec->Compress(chunk.Data));
            const auto dataBuf = compressedKeepalive.back().ToStringBuf();

            const auto keyYsonString = ConvertToYsonString(chunk.Key.Key.Underlying());
            TUnversionedRowBuilder builder;
            builder.AddValue(MakeUnversionedStringValue(chunk.Key.ComputationId.Underlying(), computationIdField));
            builder.AddValue(MakeUnversionedAnyValue(keyYsonString.AsStringBuf(), keyField));
            builder.AddValue(MakeUnversionedStringValue(chunk.Key.StreamId.Underlying(), streamIdField));
            builder.AddValue(MakeUnversionedInt64Value(chunk.Key.ChunkId, chunkIdField));
            builder.AddValue(MakeUnversionedStringValue(dataBuf, dataField));
            builder.AddValue(MakeUnversionedInt64Value(static_cast<i64>(codecId), dataCodecField));
            builder.AddValue(MakeUnversionedStringValue(chunk.ProcessedMask, processedMaskField));
            auto row = rowBuffer->CaptureRow(builder.GetRow(), /*captureValues*/ true);
            totalBytes += GetDataWeight(row);
            rows.push_back(NRowModifications::TWriteRow(row));
        }
        transaction->ModifyRows(TablePath_, nameTable, MakeSharedRange(std::move(rows), std::move(rowBuffer)));

        Metrics_.WriteRows.Increment(std::ssize(chunks));
        Metrics_.WriteBytes.Increment(totalBytes);
    }

    void UpdateMask(
        IDynamicTableTransactionPtr transaction,
        const std::vector<TMaskUpdate>& updates) override
    {
        if (updates.empty()) {
            return;
        }

        auto nameTable = New<TNameTable>();
        const i32 computationIdField = nameTable->GetIdOrRegisterName("computation_id");
        const i32 keyField = nameTable->GetIdOrRegisterName("key");
        const i32 streamIdField = nameTable->GetIdOrRegisterName("stream_id");
        const i32 chunkIdField = nameTable->GetIdOrRegisterName("chunk_id");
        const i32 processedMaskField = nameTable->GetIdOrRegisterName("processed_mask");

        auto rowBuffer = New<TRowBuffer>();
        std::vector<TRowModification> rows;
        rows.reserve(updates.size());
        i64 totalBytes = 0;
        for (const auto& update : updates) {
            const auto keyYsonString = ConvertToYsonString(update.Key.Key.Underlying());
            TUnversionedRowBuilder builder;
            builder.AddValue(MakeUnversionedStringValue(update.Key.ComputationId.Underlying(), computationIdField));
            builder.AddValue(MakeUnversionedAnyValue(keyYsonString.AsStringBuf(), keyField));
            builder.AddValue(MakeUnversionedStringValue(update.Key.StreamId.Underlying(), streamIdField));
            builder.AddValue(MakeUnversionedInt64Value(update.Key.ChunkId, chunkIdField));
            builder.AddValue(MakeUnversionedStringValue(update.ProcessedMask, processedMaskField));
            auto row = rowBuffer->CaptureRow(builder.GetRow(), /*captureValues*/ true);
            totalBytes += GetDataWeight(row);
            rows.push_back(NRowModifications::TWriteRow(row));
        }
        transaction->ModifyRows(TablePath_, nameTable, MakeSharedRange(std::move(rows), std::move(rowBuffer)));

        Metrics_.UpdateMaskRows.Increment(std::ssize(updates));
        Metrics_.UpdateMaskBytes.Increment(totalBytes);
    }

    void Erase(
        IDynamicTableTransactionPtr transaction,
        const std::vector<TTableKey>& tableKeys) override
    {
        if (tableKeys.empty()) {
            return;
        }

        auto nameTable = New<TNameTable>();
        const i32 computationIdField = nameTable->GetIdOrRegisterName("computation_id");
        const i32 keyField = nameTable->GetIdOrRegisterName("key");
        const i32 streamIdField = nameTable->GetIdOrRegisterName("stream_id");
        const i32 chunkIdField = nameTable->GetIdOrRegisterName("chunk_id");

        auto rowBuffer = New<TRowBuffer>();
        std::vector<TRowModification> rows;
        rows.reserve(tableKeys.size());
        for (const auto& tableKey : tableKeys) {
            const auto keyYsonString = ConvertToYsonString(tableKey.Key.Underlying());
            TUnversionedRowBuilder builder;
            builder.AddValue(MakeUnversionedStringValue(tableKey.ComputationId.Underlying(), computationIdField));
            builder.AddValue(MakeUnversionedAnyValue(keyYsonString.AsStringBuf(), keyField));
            builder.AddValue(MakeUnversionedStringValue(tableKey.StreamId.Underlying(), streamIdField));
            builder.AddValue(MakeUnversionedInt64Value(tableKey.ChunkId, chunkIdField));
            auto row = rowBuffer->CaptureRow(builder.GetRow(), /*captureValues*/ true);
            rows.push_back(NRowModifications::TDeleteRow(row));
        }
        transaction->ModifyRows(TablePath_, nameTable, MakeSharedRange(std::move(rows), std::move(rowBuffer)));

        Metrics_.EraseRows.Increment(std::ssize(tableKeys));
    }

private:
    struct TMetrics
    {
        NProfiling::TProfiler Profiler;
        NProfiling::TCounter SelectRows = Profiler.Counter("/select_rows");
        NProfiling::TCounter SelectBytes = Profiler.Counter("/select_bytes");
        NProfiling::TEventTimer SelectTime = Profiler.Timer("/select_time");
        NProfiling::TCounter WriteRows = Profiler.Counter("/write_rows");
        NProfiling::TCounter WriteBytes = Profiler.Counter("/write_bytes");
        NProfiling::TCounter UpdateMaskRows = Profiler.Counter("/update_mask_rows");
        NProfiling::TCounter UpdateMaskBytes = Profiler.Counter("/update_mask_bytes");
        NProfiling::TCounter EraseRows = Profiler.Counter("/erase_rows");
    };

    const TContextPtr Context_;
    TDynamicTableRequestSpecPtr DynamicSpec_;
    const NYPath::TYPath TablePath_;
    const NLogging::TLogger Logger;
    const std::string Tag_;
    const TMetrics Metrics_;
};

} // namespace

////////////////////////////////////////////////////////////////////////////////

ICompactOutputMessagesPtr CreateCompactOutputMessages(
    TContextPtr context,
    TDynamicTableRequestSpecPtr dynamicSpec)
{
    return New<TCompactOutputMessages>(std::move(context), std::move(dynamicSpec));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NTables
