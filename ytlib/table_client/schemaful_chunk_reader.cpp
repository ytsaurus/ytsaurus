#include "chunk_meta_extensions.h"
#include "chunk_state.h"
#include "config.h"
#include "private.h"
#include "schemaful_chunk_reader.h"
#include "schemaless_chunk_reader.h"

#include <yt/ytlib/chunk_client/chunk_meta_extensions.h>
#include <yt/ytlib/chunk_client/chunk_reader.h>
#include <yt/ytlib/chunk_client/chunk_spec.h>
#include <yt/ytlib/chunk_client/dispatcher.h>
#include <yt/ytlib/chunk_client/block_fetcher.h>

#include <yt/client/chunk_client/read_limit.h>

#include <yt/client/node_tracker_client/node_directory.h>

#include <yt/client/table_client/unversioned_row.h>
#include <yt/client/table_client/schema.h>
#include <yt/client/table_client/schemaful_reader.h>
#include <yt/client/table_client/name_table.h>
#include <yt/client/table_client/schemaful_reader_adapter.h>

#include <yt/ytlib/table_client/columnar_chunk_meta.h>

#include <yt/core/compression/public.h>

#include <yt/core/concurrency/scheduler.h>

#include <yt/core/logging/log.h>

#include <yt/core/misc/chunked_memory_pool.h>
#include <yt/core/misc/protobuf_helpers.h>

#include <yt/core/rpc/channel.h>

namespace NYT::NTableClient {

using namespace NConcurrency;
using namespace NChunkClient;

using NChunkClient::NProto::TMiscExt;
using NChunkClient::NProto::TChunkSpec;

////////////////////////////////////////////////////////////////////////////////

ISchemafulReaderPtr CreateSchemafulChunkReader(
    const TChunkStatePtr& chunkState,
    const TColumnarChunkMetaPtr& chunkMeta,
    TChunkReaderConfigPtr config,
    NChunkClient::IChunkReaderPtr chunkReader,
    const TClientBlockReadOptions& blockReadOptions,
    const TTableSchema& resultSchema,
    const TKeyColumns& keyColumns,
    const NChunkClient::TReadRange& readRange,
    TTimestamp timestamp)
{
    switch (chunkMeta->GetChunkFormat()) {
        case ETableChunkFormat::UnversionedColumnar:
        case ETableChunkFormat::SchemalessHorizontal: {
            auto createSchemalessReader = [=] (TNameTablePtr nameTable, TColumnFilter columnFilter) {
                return CreateSchemalessChunkReader(
                    chunkState,
                    chunkMeta,
                    std::move(config),
                    New<TChunkReaderOptions>(),
                    std::move(chunkReader),
                    std::move(nameTable),
                    blockReadOptions,
                    keyColumns,
                    columnFilter,
                    readRange);
            };

            return CreateSchemafulReaderAdapter(createSchemalessReader, resultSchema);
        }

        default:
            Y_UNREACHABLE();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
