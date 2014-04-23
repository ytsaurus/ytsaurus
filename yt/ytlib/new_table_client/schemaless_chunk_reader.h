#pragma once

#include "public.h"

#include "schemaless_reader.h"

#include <ytlib/chunk_client/chunk_reader_base.h>
#include <ytlib/chunk_client/multi_chunk_reader.h>
#include <ytlib/chunk_client/read_limit.h>

#include <ytlib/node_tracker_client/public.h>

#include <core/rpc/public.h>

namespace NYT {
namespace NVersionedTableClient {

////////////////////////////////////////////////////////////////////////////////

/*
struct IPartitionReader
{
    bool Read(std::vector<TUnversionedValue>* keyValues, std::vector<void*>* rowPointers);
};
*/

struct ISchemalessChunkReader
    : public virtual NChunkClient::IChunkReaderBase
    , public ISchemalessReader
{ };

DEFINE_REFCOUNTED_TYPE(ISchemalessChunkReader)

////////////////////////////////////////////////////////////////////////////////

ISchemalessChunkReaderPtr CreateSchemalessChunkReader(
    TChunkReaderConfigPtr config,
    NChunkClient::IAsyncReaderPtr underlyingReader,
    TNameTablePtr nameTable,
    const TKeyColumns& keyColumns,
    const NChunkClient::NProto::TChunkMeta& masterMeta,
    const NChunkClient::TReadLimit& lowerLimit,
    const NChunkClient::TReadLimit& upperLimit,
    const TColumnFilter& columnFilter,
    TNullable<int> partitionTag = Null);

////////////////////////////////////////////////////////////////////////////////

struct ISchemalessMultiChunkReader
    : public virtual NChunkClient::IMultiChunkReader
    , ISchemalessReader
{ };

DEFINE_REFCOUNTED_TYPE(ISchemalessMultiChunkReader)

////////////////////////////////////////////////////////////////////////////////

ISchemalessMultiChunkReaderPtr CreateSchemalessSequentialMultiChunkReader(
    TTableReaderConfigPtr config,
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
