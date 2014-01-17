#pragma once

#include "public.h"
#include "versioned_writer.h"

#include <ytlib/chunk_client/public.h>
#include <ytlib/chunk_client/chunk.pb.h>

namespace NYT {
namespace NVersionedTableClient {

////////////////////////////////////////////////////////////////////////////////

struct IVersionedChunkWriter
    : public IVersionedWriter
{
    virtual IVersionedWriter* GetFacade() = 0;

    virtual i64 GetMetaSize() const = 0;
    virtual i64 GetDataSize() const = 0;

    virtual NChunkClient::NProto::TChunkMeta GetMasterMeta() const = 0;
    virtual NChunkClient::NProto::TChunkMeta GetSchedulerMeta() const = 0;

};

////////////////////////////////////////////////////////////////////////////////

class TVersionedChunkProvider
    : public virtual TRefCounted
{
public:

};

////////////////////////////////////////////////////////////////////////////////

////////////////////////////////////////////////////////////////////////////////

IVersionedWriterPtr CreateVersionedChunkWriter(
    const TChunkWriterConfigPtr& config,
    const TChunkWriterOptionsPtr& options,
    const TTableSchema& schema,
    const TKeyColumns& keyColumns,
    const NChunkClient::IAsyncWriterPtr& asyncWriter);

IVersionedWriterPtr CreateVersionedChunkSequenceWriter(
    const TChunkWriterConfigPtr& config,
    const TChunkWriterOptionsPtr& options,
    const TTableSchema& schema,
    const TKeyColumns& keyColumns,
    const NChunkClient::IAsyncWriterPtr& asyncWriter);

////////////////////////////////////////////////////////////////////////////////

} // namespace NVersionedTableClient
} // namespace NYT
