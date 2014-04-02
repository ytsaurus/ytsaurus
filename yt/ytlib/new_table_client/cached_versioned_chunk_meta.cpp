#include "stdafx.h"
#include "cached_versioned_chunk_meta.h"
#include "schema.h"

#include <ytlib/chunk_client/async_reader.h>
#include <ytlib/chunk_client/dispatcher.h>

#include <core/concurrency/fiber.h>

#include <core/misc/string.h>

#include <ytlib/table_client/chunk_meta_extensions.h> // TODO(babenko): remove after migration
#include <ytlib/table_client/table_chunk_meta.pb.h>

namespace NYT {
namespace NVersionedTableClient {

using namespace NConcurrency;
using namespace NVersionedTableClient::NProto;
using namespace NChunkClient;
using namespace NChunkClient::NProto;

////////////////////////////////////////////////////////////////////////////////

TFuture<TErrorOr<TCachedVersionedChunkMetaPtr>> TCachedVersionedChunkMeta::Load(
    IAsyncReaderPtr asyncReader,
    const TTableSchema& schema,
    const TKeyColumns& keyColumns)
{
    auto cachedMeta = New<TCachedVersionedChunkMeta>();

    return BIND(&TCachedVersionedChunkMeta::DoLoad, cachedMeta)
        .AsyncVia(TDispatcher::Get()->GetReaderInvoker())
        .Run(asyncReader, schema, keyColumns);
}

TErrorOr<TCachedVersionedChunkMetaPtr> TCachedVersionedChunkMeta::DoLoad(
    IAsyncReaderPtr asyncReader,
    const TTableSchema& readerSchema,
    const TKeyColumns& keyColumns)
{
    try {
        KeyColumns_ = keyColumns;

        {
            auto error = readerSchema.CheckKeyColumns(keyColumns);
            THROW_ERROR_EXCEPTION_IF_FAILED(error);
        }

        auto chunkMetaOrError = WaitFor(asyncReader->AsyncGetChunkMeta());
        THROW_ERROR_EXCEPTION_IF_FAILED(chunkMetaOrError)
        ChunkMeta_.Swap(&chunkMetaOrError.Value());

        ValidateChunkMeta();
        ValidateSchema(readerSchema);

        Misc_ = GetProtoExtension<TMiscExt>(ChunkMeta_.extensions());
        BlockMeta_ = GetProtoExtension<TBlockMetaExt>(ChunkMeta_.extensions());
        BlockIndex_ = GetProtoExtension<TBlockIndexExt>(ChunkMeta_.extensions());
        BoundaryKeys_ = GetProtoExtension<TBoundaryKeysExt>(ChunkMeta_.extensions());

        return TErrorOr<TCachedVersionedChunkMetaPtr>(this);
    } catch (const std::exception& ex) {
        return TError("Error caching meta of chunk %s",
            ~ToString(asyncReader->GetChunkId()))
            << ex;
    }
}

void TCachedVersionedChunkMeta::ValidateChunkMeta()
{
    if (ChunkMeta_.type() != EChunkType::Table) {
        THROW_ERROR_EXCEPTION("Incorrect chunk type: actual %s, expected %s",
            ~FormatEnum(EChunkType(ChunkMeta_.type())).Quote(),
            ~FormatEnum(EChunkType(EChunkType::Table)).Quote());
    }

    if (ChunkMeta_.version() != ETableChunkFormat::VersionedSimple) {
        THROW_ERROR_EXCEPTION("Incorrect chunk format version: actual %s, expected: %s",
            ~FormatEnum(ETableChunkFormat(ChunkMeta_.version())).Quote(),
            ~FormatEnum(ETableChunkFormat(ETableChunkFormat::VersionedSimple)).Quote());
    }
}

void TCachedVersionedChunkMeta::ValidateSchema(const TTableSchema& readerSchema)
{
    auto chunkKeyColumnsExt = GetProtoExtension<TKeyColumnsExt>(ChunkMeta_.extensions());
    auto chunkKeyColumns = NYT::FromProto<TKeyColumns>(chunkKeyColumnsExt);   
    if (KeyColumns_ != chunkKeyColumns) {
        THROW_ERROR_EXCEPTION("Incorrect key columns: actual [%s], expected [%s]",
            ~JoinToString(chunkKeyColumns),
            ~JoinToString(KeyColumns_));
    }

    auto protoSchema = GetProtoExtension<TTableSchemaExt>(ChunkMeta_.extensions());
    FromProto(&ChunkSchema_, protoSchema);

    SchemaIdMapping_.reserve(readerSchema.Columns().size() - KeyColumns_.size());
    for (int readerIndex = KeyColumns_.size(); readerIndex < readerSchema.Columns().size(); ++readerIndex) {
        auto& column = readerSchema.Columns()[readerIndex];
        auto* chunkColumn = ChunkSchema_.FindColumn(column.Name);
        if (!chunkColumn) {
            // ToDo (psushin): this may be a valid case, just skip the column.
            THROW_ERROR_EXCEPTION(
                "Incompatible schema: column %s is absent in chunk schema",
                ~column.Name.Quote());
        }

        if (chunkColumn->Type != column.Type) {
            THROW_ERROR_EXCEPTION("Incompatible type for column %s: actual: %s, expected %s",
                ~FormatEnum(chunkColumn->Type).Quote(),
                ~FormatEnum(column.Type).Quote());
        }

        TColumnIdMapping mapping;
        mapping.ChunkSchemaIndex = ChunkSchema_.GetColumnIndex(*chunkColumn);
        mapping.ReaderSchemaIndex = readerIndex;
        SchemaIdMapping_.push_back(mapping);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NVersionedTableClient
} // namespace NYT
