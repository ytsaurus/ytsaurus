#include "blob_table_writer.h"
#include "helpers.h"
#include "schemaless_chunk_writer.h"

#include <yt/yt/ytlib/chunk_client/config.h>

#include <yt/yt/ytlib/table_client/config.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/client/table_client/unversioned_row.h>
#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/private.h>
#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/helpers.h>

namespace NYT::NTableClient {

using namespace NYson;
using namespace NConcurrency;
using namespace NCypressClient;
using namespace NChunkClient;

////////////////////////////////////////////////////////////////////////////////

TBlobTableWriter::TBlobTableWriter(
    const TBlobTableSchema& blobTableSchema,
    const std::vector<TYsonString>& blobIdColumnValues,
    NApi::NNative::IClientPtr client,
    TBlobTableWriterConfigPtr blobTableWriterConfig,
    TTableWriterOptionsPtr tableWriterOptions,
    TTransactionId transactionId,
    TMasterTableSchemaId schemaId,
    const std::optional<NChunkClient::TDataSink>& dataSink,
    TChunkListId chunkListId,
    TTrafficMeterPtr trafficMeter,
    IThroughputThrottlerPtr throttler,
    IChunkWriter::TWriteBlocksOptions writeBlocksOptions)
    : PartSize_(blobTableWriterConfig->MaxPartSize)
    , Logger(TableClientLogger().WithTag("TransactionId: %v, ChunkListId: %v",
        transactionId,
        chunkListId))
{
    YT_LOG_INFO("Creating blob writer");

    Buffer_.Reserve(PartSize_);

    auto tableSchema = blobTableSchema.ToTableSchema();
    auto nameTable = TNameTable::FromSchema(*tableSchema);

    for (const auto& column : blobTableSchema.BlobIdColumns) {
        BlobIdColumnIds_.push_back(nameTable->GetIdOrThrow(column.Name()));
    }

    TUnversionedOwningRowBuilder builder;

    YT_VERIFY(blobIdColumnValues.size() == blobTableSchema.BlobIdColumns.size());
    for (size_t i = 0; i < BlobIdColumnIds_.size(); ++i) {
        builder.AddValue(TryDecodeUnversionedAnyValue(MakeUnversionedAnyValue(blobIdColumnValues[i].AsStringBuf(), BlobIdColumnIds_[i])));
    }
    BlobIdColumnValues_ = builder.FinishRow();

    PartIndexColumnId_ = nameTable->GetIdOrThrow(blobTableSchema.PartIndexColumn);
    DataColumnId_ = nameTable->GetIdOrThrow(blobTableSchema.DataColumn);

    MultiChunkWriter_ = CreateSchemalessMultiChunkWriter(
        blobTableWriterConfig,
        tableWriterOptions,
        nameTable,
        tableSchema,
        TLegacyOwningKey(),
        client,
        /*localHostName*/ std::string(), // Locality is not important for table upload.
        NObjectClient::CellTagFromId(chunkListId),
        transactionId,
        schemaId,
        dataSink,
        std::move(writeBlocksOptions),
        chunkListId,
        TChunkTimestamps(),
        trafficMeter,
        throttler);
}

NControllerAgent::NProto::TOutputResult TBlobTableWriter::GetOutputResult(bool withChunkSpecs) const
{
    if (Failed_) {
        NControllerAgent::NProto::TOutputResult boundaryKeys;
        boundaryKeys.set_empty(true);
        return boundaryKeys;
    } else {
        return GetWrittenChunksBoundaryKeys(MultiChunkWriter_, withChunkSpecs);
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
