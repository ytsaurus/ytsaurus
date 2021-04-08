#include "chunk_meta_extensions.h"
#include "chunk_state.h"
#include "config.h"
#include "private.h"
#include "schemaful_chunk_reader.h"
#include "schemaless_multi_chunk_reader.h"

#include <yt/yt/ytlib/chunk_client/chunk_meta_extensions.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader.h>
#include <yt/yt/ytlib/chunk_client/chunk_spec.h>
#include <yt/yt/ytlib/chunk_client/dispatcher.h>
#include <yt/yt/ytlib/chunk_client/block_fetcher.h>

#include <yt/yt/client/chunk_client/read_limit.h>

#include <yt/yt/client/node_tracker_client/node_directory.h>

#include <yt/yt/client/table_client/unversioned_row.h>
#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/unversioned_reader.h>
#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/schemaful_reader_adapter.h>

#include <yt/yt/ytlib/table_client/columnar_chunk_meta.h>

#include <yt/yt/core/compression/public.h>

#include <yt/yt/core/concurrency/scheduler.h>

#include <yt/yt/core/logging/log.h>

#include <yt/yt/core/misc/chunked_memory_pool.h>
#include <yt/yt/core/misc/protobuf_helpers.h>

#include <yt/yt/core/rpc/channel.h>

namespace NYT::NTableClient {

using namespace NConcurrency;
using namespace NChunkClient;

using NChunkClient::NProto::TMiscExt;
using NChunkClient::NProto::TChunkSpec;

////////////////////////////////////////////////////////////////////////////////

ISchemafulUnversionedReaderPtr CreateSchemafulChunkReader(
    const TChunkStatePtr& chunkState,
    const TColumnarChunkMetaPtr& chunkMeta,
    TChunkReaderConfigPtr config,
    NChunkClient::IChunkReaderPtr chunkReader,
    const TClientChunkReadOptions& chunkReadOptions,
    const TTableSchemaPtr& resultSchema,
    const TSortColumns& sortColumns,
    const NChunkClient::TReadRange& readRange,
    TTimestamp timestamp)
{
    switch (chunkMeta->GetChunkFormat()) {
        case ETableChunkFormat::UnversionedColumnar:
        case ETableChunkFormat::SchemalessHorizontal: {
            auto createSchemalessReader = [=] (TNameTablePtr nameTable, TColumnFilter columnFilter) {
                return CreateSchemalessRangeChunkReader(
                    chunkState,
                    chunkMeta,
                    std::move(config),
                    TChunkReaderOptions::GetDefault(),
                    std::move(chunkReader),
                    std::move(nameTable),
                    chunkReadOptions,
                    sortColumns,
                    /* omittedInaccessibleColumns */ {},
                    columnFilter,
                    readRange);
            };

            return CreateSchemafulReaderAdapter(createSchemalessReader, resultSchema);
        }

        default:
            YT_ABORT();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
