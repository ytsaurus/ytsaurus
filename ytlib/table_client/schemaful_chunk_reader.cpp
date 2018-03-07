#include "chunk_meta_extensions.h"
#include "chunk_state.h"
#include "config.h"
#include "name_table.h"
#include "private.h"
#include "schema.h"
#include "schemaful_chunk_reader.h"
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
    const TReadSessionId& sessionId,
    const TTableSchema& resultSchema,
    const TKeyColumns& keyColumns,
    const NChunkClient::NProto::TChunkMeta& chunkMeta,
    const TReadRange& readRange,
    TTimestamp timestamp)
{
    auto type = EChunkType(chunkMeta.type());
    YCHECK(type == EChunkType::Table);

    auto formatVersion = ETableChunkFormat(chunkMeta.version());

    TChunkSpec chunkSpec;
    chunkSpec.mutable_chunk_meta()->MergeFrom(chunkMeta);

    switch (formatVersion) {
        case ETableChunkFormat::SchemalessHorizontal:
        case ETableChunkFormat::UnversionedColumnar: {
            auto createSchemalessReader = [=] (TNameTablePtr nameTable, TColumnFilter columnFilter) {
                auto chunkState = New<TChunkState>(
                    std::move(blockCache),
                    chunkSpec,
                    nullptr,
                    nullptr,
                    nullptr,
                    nullptr);

                return CreateSchemalessChunkReader(
                    std::move(chunkState),
                    std::move(config),
                    New<TChunkReaderOptions>(),
                    std::move(chunkReader),
                    std::move(nameTable),
                    sessionId,
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

ISchemafulReaderPtr CreateSchemafulChunkReader(
    TChunkReaderConfigPtr config,
    NChunkClient::IChunkReaderPtr chunkReader,
    IBlockCachePtr blockCache,
    const TReadSessionId& sessionId,
    const TTableSchema& resultSchema,
    const TKeyColumns& keyColumns,
    const NChunkClient::NProto::TChunkMeta& chunkMeta,
    const TSharedRange<TKey>& keys,
    TTimestamp timestamp)
{
    auto type = EChunkType(chunkMeta.type());
    YCHECK(type == EChunkType::Table);

    auto formatVersion = ETableChunkFormat(chunkMeta.version());

    TChunkSpec chunkSpec;
    chunkSpec.mutable_chunk_meta()->MergeFrom(chunkMeta);

    switch (formatVersion) {
        case ETableChunkFormat::UnversionedColumnar:
        case ETableChunkFormat::SchemalessHorizontal: {
            auto createSchemalessReader = [=] (TNameTablePtr nameTable, TColumnFilter columnFilter) {
                auto chunkState = New<TChunkState>(
                    std::move(blockCache),
                    chunkSpec,
                    nullptr,
                    nullptr,
                    nullptr,
                    nullptr);

                return CreateSchemalessChunkReader(
                    std::move(chunkState),
                    std::move(config),
                    New<TChunkReaderOptions>(),
                    std::move(chunkReader),
                    std::move(nameTable),
                    sessionId,
                    keyColumns,
                    columnFilter,
                    keys);
            };

            return CreateSchemafulReaderAdapter(createSchemalessReader, resultSchema);
        }

        default:
            Y_UNREACHABLE();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
