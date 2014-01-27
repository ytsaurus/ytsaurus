#pragma once

#include "public.h"
#include "versioned_chunk_writer.h"

#include <ytlib/chunk_client/multi_chunk_sequential_writer.h>

namespace NYT {
namespace NVersionedTableClient {

////////////////////////////////////////////////////////////////////////////////

class TVersionedMultiChunkWriter
    : public NChunkClient::TMultiChunkSequentialWriter<TVersionedChunkWriterProvider>
    , public IVersionedWriter
{
    typedef NChunkClient::TMultiChunkSequentialWriter<TVersionedChunkWriterProvider> TBase;

public:
    TVersionedMultiChunkWriter(
        NChunkClient::TMultiChunkWriterConfigPtr config,
        NChunkClient::TMultiChunkWriterOptionsPtr options,
        TVersionedChunkWriterProviderPtr provider,
        NRpc::IChannelPtr masterChannel,
        const NTransactionClient::TTransactionId& transactionId,
        const NChunkClient::TChunkListId& parentChunkListId = NChunkClient::NullChunkListId);

    virtual bool Write(const std::vector<TVersionedRow>& rows) override;

private:
    IVersionedWriter* CurrentWriter_;

    // Hides method in base class.
    IVersionedWriter* GetCurrentWriter();

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NVersionedTableClient
} // namespace NYT
