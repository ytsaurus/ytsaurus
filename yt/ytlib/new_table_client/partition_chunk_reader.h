#pragma once

#include "public.h"

#include <ytlib/chunk_client/chunk_reader_base.h>
#include <ytlib/chunk_client/multi_chunk_reader.h>

#include <ytlib/node_tracker_client/public.h>

#include <ytlib/transaction_client/public.h>

#include <core/rpc/public.h>

namespace NYT {
namespace NVersionedTableClient {

////////////////////////////////////////////////////////////////////////////////

struct IPartitionReader
    : public virtual NChunkClient::IReaderBase
{
    typedef std::back_insert_iterator<std::vector<TUnversionedValue>> TValueIterator;
    typedef std::back_insert_iterator<std::vector<const char*>> TRowPointerIterator;

    virtual bool Read(
        i64 maxRowCount,
        TValueIterator& valueInserter,
        TRowPointerIterator& rowPointerInserter,
        i64* rowCount) = 0;
};

DEFINE_REFCOUNTED_TYPE(IPartitionReader)

////////////////////////////////////////////////////////////////////////////////

struct IPartitionChunkReader
    : public virtual NChunkClient::IChunkReaderBase
    , public IPartitionReader
{ };

DEFINE_REFCOUNTED_TYPE(IPartitionChunkReader)

////////////////////////////////////////////////////////////////////////////////

IPartitionChunkReaderPtr CreatePartitionChunkReader(
    TChunkReaderConfigPtr config,
    NChunkClient::IAsyncReaderPtr underlyingReader,
    TNameTablePtr nameTable,
    const TKeyColumns& keyColumns,
    const NChunkClient::NProto::TChunkMeta& masterMeta,
    int partitionTag);

////////////////////////////////////////////////////////////////////////////////

struct IPartitionMultiChunkReader
    : public virtual NChunkClient::IMultiChunkReader
    , IPartitionReader
{ };

DEFINE_REFCOUNTED_TYPE(IPartitionMultiChunkReader)

////////////////////////////////////////////////////////////////////////////////

IPartitionMultiChunkReaderPtr CreatePartitionParallelMultiChunkReader(
    NChunkClient::TMultiChunkReaderConfigPtr config,
    NChunkClient::TMultiChunkReaderOptionsPtr options,
    NRpc::IChannelPtr masterChannel,
    NChunkClient::IBlockCachePtr blockCache,
    NNodeTrackerClient::TNodeDirectoryPtr nodeDirectory,
    const std::vector<NChunkClient::NProto::TChunkSpec>& chunkSpecs,
    TNameTablePtr nameTable,
    const TKeyColumns& keyColumns);

////////////////////////////////////////////////////////////////////////////////

} // namespace NVersionedTableClient
} // namespace NYT
