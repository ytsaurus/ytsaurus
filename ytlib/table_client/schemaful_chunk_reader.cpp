#include "schemaful_chunk_reader.h"
#include "private.h"
#include "chunk_meta_extensions.h"
#include "config.h"
#include "name_table.h"
#include "schema.h"
#include "schemaful_block_reader.h"
#include "schemaful_reader.h"
#include "schemaful_reader_adapter.h"
#include "schemaless_chunk_reader.h"
#include "unversioned_row.h"

#include <yt/ytlib/chunk_client/chunk_meta_extensions.h>
#include <yt/ytlib/chunk_client/chunk_reader.h>
#include <yt/ytlib/chunk_client/chunk_spec.h>
#include <yt/ytlib/chunk_client/dispatcher.h>
#include <yt/ytlib/chunk_client/read_limit.h>
#include <yt/ytlib/chunk_client/block_fetcher.h>

#include <yt/ytlib/node_tracker_client/node_directory.h>

#include <yt/core/compression/public.h>

#include <yt/core/concurrency/scheduler.h>

#include <yt/core/logging/log.h>

#include <yt/core/misc/async_stream_state.h>
#include <yt/core/misc/chunked_memory_pool.h>
#include <yt/core/misc/protobuf_helpers.h>

#include <yt/core/rpc/channel.h>

namespace NYT {
namespace NTableClient {

using namespace NConcurrency;
using namespace NChunkClient;

using NChunkClient::NProto::TMiscExt;
using NChunkClient::NProto::TChunkSpec;

////////////////////////////////////////////////////////////////////////////////

ISchemafulReaderPtr CreateSchemafulChunkReader(
    TChunkReaderConfigPtr config,
    NChunkClient::IChunkReaderPtr chunkReader,
    IBlockCachePtr blockCache,
    const TTableSchema& schema,
    const NChunkClient::NProto::TChunkMeta& chunkMeta,
    std::vector<TReadRange> readRanges,
    TTimestamp timestamp)
{
    auto type = EChunkType(chunkMeta.type());
    YCHECK(type == EChunkType::Table);

    auto formatVersion = ETableChunkFormat(chunkMeta.version());

    TChunkSpec chunkSpec;
    chunkSpec.mutable_chunk_meta()->MergeFrom(chunkMeta);
    
    switch (formatVersion) {
        case ETableChunkFormat::Old:
        case ETableChunkFormat::SchemalessHorizontal: {
            auto createSchemalessReader = [=] (TNameTablePtr nameTable, TColumnFilter columnFilter) {
                return CreateSchemalessChunkReader(
                    chunkSpec,
                    std::move(config),
                    New<TChunkReaderOptions>(),
                    std::move(chunkReader),
                    std::move(nameTable),
                    std::move(blockCache),
                    TKeyColumns(),
                    columnFilter,
                    std::move(readRanges));
            };

            return CreateSchemafulReaderAdapter(createSchemalessReader, schema);
        }

        default:
            YUNREACHABLE();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
