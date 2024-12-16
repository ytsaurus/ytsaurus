#include "chunk_meta_extensions.h"
#include "chunk_state.h"
#include "config.h"
#include "schemaful_chunk_reader.h"
#include "schemaless_multi_chunk_reader.h"
#include "schemaful_reader_adapter.h"

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
#include <yt/yt/client/table_client/private.h>

#include <yt/yt/ytlib/table_client/columnar_chunk_meta.h>

#include <yt/yt/core/compression/public.h>

#include <yt/yt/core/concurrency/scheduler.h>

#include <yt/yt/core/logging/log.h>

#include <yt/yt/core/misc/protobuf_helpers.h>

#include <yt/yt/core/rpc/channel.h>

#include <library/cpp/yt/memory/chunked_memory_pool.h>

namespace NYT::NTableClient {

using namespace NConcurrency;
using namespace NChunkClient;
using namespace NQueryClient;

////////////////////////////////////////////////////////////////////////////////

ISchemafulUnversionedReaderPtr CreateSchemafulChunkReader(
    const IColumnEvaluatorCachePtr& columnEvaluatorCache,
    const TChunkStatePtr& chunkState,
    const TColumnarChunkMetaPtr& chunkMeta,
    TChunkReaderConfigPtr config,
    IChunkReaderPtr chunkReader,
    const TClientChunkReadOptions& chunkReadOptions,
    const TTableSchemaPtr& resultSchema,
    const TSortColumns& sortColumns,
    const TReadRange& readRange)
{
    switch (chunkMeta->GetChunkFormat()) {
        case EChunkFormat::TableUnversionedColumnar:
        case EChunkFormat::TableUnversionedSchemalessHorizontal: {
            auto createSchemalessReader = [=] (TNameTablePtr nameTable, const TColumnFilter& columnFilter) {
                return CreateSchemalessRangeChunkReader(
                    columnEvaluatorCache,
                    chunkState,
                    chunkMeta,
                    std::move(config),
                    TChunkReaderOptions::GetDefault(),
                    std::move(chunkReader),
                    std::move(nameTable),
                    chunkReadOptions,
                    sortColumns,
                    /*omittedInaccessibleColumns*/ {},
                    columnFilter,
                    readRange);
            };

            return CreateSchemafulReaderAdapter(
                createSchemalessReader,
                resultSchema,
                /*columnFilter*/ {},
                /*ignoreRequired*/ false,
                chunkReadOptions.MemoryUsageTracker);
        }

        default:
            YT_ABORT();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
