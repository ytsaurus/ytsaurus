#include "stdafx.h"
#include "versioned_multi_chunk_writer.h"

namespace NYT {
namespace NVersionedTableClient {

////////////////////////////////////////////////////////////////////////////////

TVersionedMultiChunkWriter::TVersionedMultiChunkWriter(
    NChunkClient::TMultiChunkWriterConfigPtr config,
    NChunkClient::TMultiChunkWriterOptionsPtr options,
    TVersionedChunkWriterProviderPtr provider,
    NRpc::IChannelPtr masterChannel,
    const NTransactionClient::TTransactionId& transactionId,
    const NChunkClient::TChunkListId& parentChunkListId)
    : TBase(config, options, provider, masterChannel, transactionId, parentChunkListId)
    , CurrentWriter_(nullptr)
{ }

bool TVersionedMultiChunkWriter::Write(const std::vector<TVersionedRow> &rows)
{
    if (!CurrentWriter_) {
        YCHECK(CurrentWriter_ = GetCurrentWriter());
    }

    bool res = CurrentWriter_->Write(rows);
    return (CurrentWriter_ = GetCurrentWriter()) && res;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NVersionedTableClient
} // namespace NYT
