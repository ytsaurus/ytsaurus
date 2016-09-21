#include "blob_table_writer.h"

#include "helpers.h"
#include "name_table.h"
#include "private.h"
#include "public.h"
#include "schemaless_chunk_writer.h"

#include <yt/ytlib/object_client/helpers.h>
#include <yt/ytlib/table_client/unversioned_row.h>

namespace NYT {
namespace NTableClient {

static const auto& Logger = TableClientLogger;

using NConcurrency::WaitFor;
using NCypressClient::TTransactionId;
using NChunkClient::TChunkListId;

////////////////////////////////////////////////////////////////////////////////

TTableSchema TBlobTableSchema::ToTableSchema() const
{
    std::vector<TColumnSchema> columns;
    for (const auto& idColumn : BlobIdColumns) {
        columns.emplace_back(idColumn, EValueType::String);
        columns.back().SetSortOrder(ESortOrder::Ascending);
    }
    columns.emplace_back(PartIndexColumn, EValueType::Int64);
    columns.back().SetSortOrder(ESortOrder::Ascending);
    columns.emplace_back(DataColumn, EValueType::String);
    return TTableSchema(
        std::move(columns),
        true, // strict
        true); // uniqueKeys
}

////////////////////////////////////////////////////////////////////////////////

TBlobTableWriter::TBlobTableWriter(
    const TBlobTableSchema& blobTableSchema,
    std::vector<Stroka> blobIdColumnValues,
    NApi::IClientPtr client,
    TBlobTableWriterConfigPtr blobTableWriterConfig,
    TTableWriterOptionsPtr tableWriterOptions,
    const TTransactionId& transactionId,
    const TChunkListId& chunkListId)
    : BlobIdColumnValues_(std::move(blobIdColumnValues))
    , PartSize_(blobTableWriterConfig->MaxPartSize)
{
    YCHECK(BlobIdColumnValues_.size() == blobTableSchema.BlobIdColumns.size());

    LOG_INFO("Creating blob writer (TransactionId: %v, ChunkListId %v)",
        transactionId,
        chunkListId);

    Buffer_.Reserve(PartSize_);

    auto tableSchema = blobTableSchema.ToTableSchema();
    auto nameTable = TNameTable::FromSchema(tableSchema);

    for (const auto& column : blobTableSchema.BlobIdColumns) {
        BlobIdColumnIds_.emplace_back(nameTable->GetIdOrThrow(column));
    }

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
        chunkListId);

    WaitFor(MultiChunkWriter_->Open())
        .ThrowOnError();
}

TBlobTableWriter::~TBlobTableWriter()
{
    DoFinish();
}

NScheduler::NProto::TOutputResult TBlobTableWriter::GetOutputResult() const
{
    return GetWrittenChunksBoundaryKeys(MultiChunkWriter_);
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
    if (Buffer_.Size() == 0) {
        return;
    }

    auto dataBuf = TStringBuf(Buffer_.Begin(), Buffer_.Size());

    const size_t columnCount = BlobIdColumnIds_.size() + 2;

    TUnversionedRowBuilder builder(columnCount);
    for (size_t i = 0; i < BlobIdColumnIds_.size(); ++i) {
        builder.AddValue(MakeUnversionedStringValue(BlobIdColumnValues_[i], BlobIdColumnIds_[i]));
    }
    builder.AddValue(MakeUnversionedInt64Value(WrittenPartCount_, PartIndexColumnId_));
    builder.AddValue(MakeUnversionedStringValue(dataBuf, DataColumnId_));

    ++WrittenPartCount_;

    if (!MultiChunkWriter_->Write({builder.GetRow()})) {
        WaitFor(MultiChunkWriter_->GetReadyEvent())
            .ThrowOnError();
    }
    Buffer_.Clear();
}

void TBlobTableWriter::DoFinish()
{
    if (Finished_) {
        return;
    }
    Flush();
    WaitFor(MultiChunkWriter_->Close())
        .ThrowOnError();
    Finished_ = true;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
