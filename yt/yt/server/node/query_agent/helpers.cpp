#include "helpers.h"

#include <yt/yt/server/node/data_node/public.h>

#include <yt/yt/client/tablet_client/public.h>

namespace NYT::NQueryAgent {

using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

bool IsRetriableQueryError(const TError& error)
{
    return
        error.FindMatching(NDataNode::EErrorCode::LocalChunkReaderFailed) ||
        error.FindMatching(NChunkClient::EErrorCode::NoSuchChunk) ||
        error.FindMatching(NTabletClient::EErrorCode::TabletSnapshotExpired);
}

////////////////////////////////////////////////////////////////////////////////

TSimpleRowsetWriter::TSimpleRowsetWriter(IMemoryChunkProviderPtr chunkProvider)
    : RowBuffer_(New<TRowBuffer>(TSchemafulRowsetWriterBufferTag(), std::move(chunkProvider)))
{ }

TSharedRange<TUnversionedRow> TSimpleRowsetWriter::GetRows() const
{
    return MakeSharedRange(Rows_, RowBuffer_);
}

TFuture<TSharedRange<TUnversionedRow>> TSimpleRowsetWriter::GetResult() const
{
    return Result_.ToFuture();
}

TFuture<void> TSimpleRowsetWriter::Close()
{
    Result_.TrySet(GetRows());
    return VoidFuture;
}

bool TSimpleRowsetWriter::Write(TRange<TUnversionedRow> rows)
{
    for (auto row : rows) {
        Rows_.push_back(RowBuffer_->CaptureRow(row));
    }
    return true;
}

TFuture<void> TSimpleRowsetWriter::GetReadyEvent()
{
    return VoidFuture;
}

void TSimpleRowsetWriter::Fail(const TError& error)
{
    Result_.TrySet(error);
}

std::optional<NCrypto::TMD5Hash> TSimpleRowsetWriter::GetDigest() const
{
    return std::nullopt;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryAgent
