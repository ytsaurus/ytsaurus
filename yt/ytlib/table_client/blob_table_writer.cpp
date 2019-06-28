#include "blob_table_writer.h"
#include "helpers.h"
#include "private.h"
#include "schemaless_chunk_writer.h"

#include <yt/ytlib/chunk_client/config.h>

#include <yt/ytlib/table_client/config.h>

#include <yt/core/yson/lexer.h>

#include <yt/client/object_client/helpers.h>

#include <yt/client/table_client/unversioned_row.h>
#include <yt/client/table_client/name_table.h>
#include <yt/client/table_client/schema.h>

namespace NYT::NTableClient {

using namespace NYson;
using namespace NConcurrency;

using NConcurrency::WaitFor;
using NCypressClient::TTransactionId;
using NChunkClient::TChunkListId;
using NChunkClient::TTrafficMeterPtr;
using NLogging::TLogger;

////////////////////////////////////////////////////////////////////////////////

TBlobTableWriter::TBlobTableWriter(
    const TBlobTableSchema& blobTableSchema,
    const std::vector<TYsonString>& blobIdColumnValues,
    NApi::NNative::IClientPtr client,
    TBlobTableWriterConfigPtr blobTableWriterConfig,
    TTableWriterOptionsPtr tableWriterOptions,
    TTransactionId transactionId,
    TChunkListId chunkListId,
    TTrafficMeterPtr trafficMeter,
    IThroughputThrottlerPtr throttler)
    : PartSize_(blobTableWriterConfig->MaxPartSize)
    , Logger(TLogger(TableClientLogger)
        .AddTag("TransactionId: %v", transactionId)
        .AddTag("ChunkListId: %v", chunkListId))
{

    YT_LOG_INFO("Creating blob writer");

    Buffer_.Reserve(PartSize_);

    auto tableSchema = blobTableSchema.ToTableSchema();
    auto nameTable = TNameTable::FromSchema(tableSchema);

    for (const auto& column : blobTableSchema.BlobIdColumns) {
        BlobIdColumnIds_.emplace_back(nameTable->GetIdOrThrow(column.Name()));
    }

    TStatelessLexer lexer;
    TUnversionedOwningRowBuilder builder;

    YT_VERIFY(blobIdColumnValues.size() == blobTableSchema.BlobIdColumns.size());
    for (size_t i = 0; i < BlobIdColumnIds_.size(); ++i) {
        builder.AddValue(MakeUnversionedValue(blobIdColumnValues[i].GetData(), BlobIdColumnIds_[i], lexer));
    }
    BlobIdColumnValues_ = builder.FinishRow();

    PartIndexColumnId_ = nameTable->GetIdOrThrow(blobTableSchema.PartIndexColumn);
    DataColumnId_ = nameTable->GetIdOrThrow(blobTableSchema.DataColumn);

    MultiChunkWriter_ = CreateSchemalessMultiChunkWriter(
        blobTableWriterConfig,
        tableWriterOptions,
        nameTable,
        tableSchema,
        TOwningKey(),
        client,
        NObjectClient::CellTagFromId(chunkListId),
        transactionId,
        chunkListId,
        TChunkTimestamps(),
        trafficMeter,
        throttler);
}

NScheduler::NProto::TOutputResult TBlobTableWriter::GetOutputResult() const
{
    if (Failed_) {
        NScheduler::NProto::TOutputResult boundaryKeys;
        boundaryKeys.set_empty(true);
        return boundaryKeys;
    } else {
        return GetWrittenChunksBoundaryKeys(MultiChunkWriter_);
    }
}

void TBlobTableWriter::DoWrite(const void* buf_, size_t size)
{
    const char* buf = static_cast<const char*>(buf_);
    while (size > 0) {
        const size_t remainingCapacity = Buffer_.Capacity() - Buffer_.Size();
        const size_t toWrite = std::min(size, remainingCapacity);
        Buffer_.Write(buf, toWrite);
        buf += toWrite;
        size -= toWrite;
        if (Buffer_.Size() == Buffer_.Capacity()) {
            Flush();
        }
    }
}

void TBlobTableWriter::DoFlush()
{
    if (Buffer_.Size() == 0 || Failed_) {
        return;
    }

    auto dataBuf = TStringBuf(Buffer_.Begin(), Buffer_.Size());

    const size_t columnCount = BlobIdColumnIds_.size() + 2;

    TUnversionedRowBuilder builder(columnCount);
    for (const auto* value = BlobIdColumnValues_.Begin(); value != BlobIdColumnValues_.End(); value++) {
        builder.AddValue(*value);
    }
    builder.AddValue(MakeUnversionedInt64Value(WrittenPartCount_, PartIndexColumnId_));
    builder.AddValue(MakeUnversionedStringValue(dataBuf, DataColumnId_));

    ++WrittenPartCount_;

    if (!MultiChunkWriter_->Write({builder.GetRow()})) {
        auto error = WaitFor(MultiChunkWriter_->GetReadyEvent());
        if (!error.IsOK()) {
            Failed_ = true;
            YT_LOG_WARNING(error, "Blob table writer failed");
            error.ThrowOnError();
        }
    }

    Buffer_.Clear();
}

void TBlobTableWriter::DoFinish()
{
    if (Finished_ || Failed_) {
        return;
    }
    Finished_ = true;
    Flush();

    auto error = WaitFor(MultiChunkWriter_->Close());
    if (!error.IsOK()) {
        Failed_ = true;
        YT_LOG_WARNING(error, "Blob table writer failed");
        error.ThrowOnError();
    }

    YT_LOG_DEBUG("Blob table writer finished");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
