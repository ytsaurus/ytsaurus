#pragma once

#include "public.h"
#include "chunk_meta_extensions.h"
#include "schema.h"
#include "versioned_writer.h"

#include <ytlib/chunk_client/public.h>
#include <ytlib/chunk_client/chunk_writer_base.h>
#include <ytlib/chunk_client/multi_chunk_sequential_writer_base.h>

namespace NYT {
namespace NVersionedTableClient {

////////////////////////////////////////////////////////////////////////////////

struct IVersionedChunkWriter
    : public IVersionedWriter
    , public NChunkClient::IChunkWriterBase
{ };

DEFINE_REFCOUNTED_TYPE(IVersionedChunkWriter)

////////////////////////////////////////////////////////////////////////////////

IVersionedChunkWriterPtr CreateVersionedChunkWriter(
    TChunkWriterConfigPtr config,
    TChunkWriterOptionsPtr options,
    const TTableSchema& schema,
    const TKeyColumns& keyColumns,
    NChunkClient::IAsyncWriterPtr asyncWriter);

////////////////////////////////////////////////////////////////////////////////

class TVersionedMultiChunkWriter
    : public NChunkClient::TMultiChunkSequentialWriterBase
    , public IVersionedWriter
{
public:
    TVersionedMultiChunkWriter(
        TTableWriterConfigPtr config,
        TTableWriterOptionsPtr options,
        const TTableSchema& schema,
        const TKeyColumns& keyColumns,
        NRpc::IChannelPtr masterChannel,
        const NTransactionClient::TTransactionId& transactionId,
        const NChunkClient::TChunkListId& parentChunkListId = NChunkClient::NullChunkListId);

    virtual bool Write(const std::vector<TVersionedRow>& rows) override;

private:
    TTableWriterConfigPtr Config_;
    TTableWriterOptionsPtr Options_;
    TTableSchema Schema_;
    TKeyColumns KeyColumns_;

    IVersionedWriter* CurrentWriter_;


    virtual NChunkClient::IChunkWriterBasePtr CreateFrontalWriter(NChunkClient::IAsyncWriterPtr underlyingWriter) override;

};

DEFINE_REFCOUNTED_TYPE(TVersionedMultiChunkWriter)

////////////////////////////////////////////////////////////////////////////////

} // namespace NVersionedTableClient
} // namespace NYT
