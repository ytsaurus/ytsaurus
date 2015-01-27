#include "stdafx.h"
#include "cached_versioned_chunk_meta.h"
#include "schema.h"

#include <ytlib/chunk_client/chunk_reader.h>
#include <ytlib/chunk_client/dispatcher.h>

#include <core/concurrency/scheduler.h>

#include <core/misc/string.h>

#include <ytlib/table_client/chunk_meta_extensions.h> // TODO(babenko): remove after migration
#include <ytlib/table_client/table_chunk_meta.pb.h>

namespace NYT {
namespace NVersionedTableClient {

using namespace NConcurrency;
using namespace NVersionedTableClient::NProto;
using namespace NChunkClient;
using namespace NChunkClient::NProto;

using NYT::FromProto;

////////////////////////////////////////////////////////////////////////////////

TFuture<TCachedVersionedChunkMetaPtr> TCachedVersionedChunkMeta::Load(
    IChunkReaderPtr chunkReader,
    const TTableSchema& schema,
    const TKeyColumns& keyColumns)
{
    auto cachedMeta = New<TCachedVersionedChunkMeta>();
    return BIND(&TCachedVersionedChunkMeta::DoLoad, cachedMeta)
        .AsyncVia(TDispatcher::Get()->GetReaderInvoker())
        .Run(chunkReader, schema, keyColumns);
}

TCachedVersionedChunkMetaPtr TCachedVersionedChunkMeta::DoLoad(
    IChunkReaderPtr chunkReader,
    const TTableSchema& readerSchema,
    const TKeyColumns& keyColumns)
{
    try {
        KeyColumns_ = keyColumns;

        ValidateTableSchemaAndKeyColumns(readerSchema, keyColumns);

        auto asyncChunkMeta = chunkReader->GetMeta();
        ChunkMeta_ = WaitFor(asyncChunkMeta)
            .ValueOrThrow();

        ValidateChunkMeta();
        ValidateSchema(readerSchema);

        auto boundaryKeysExt = GetProtoExtension<TBoundaryKeysExt>(ChunkMeta_.extensions());
        MinKey_ = FromProto<TOwningKey>(boundaryKeysExt.min());
        MaxKey_ = FromProto<TOwningKey>(boundaryKeysExt.max());

        auto blockIndexExt = GetProtoExtension<TBlockIndexExt>(ChunkMeta_.extensions());
        BlockIndexKeys_.reserve(blockIndexExt.entries_size());
        for (const auto& protoKey : blockIndexExt.entries()) {
            BlockIndexKeys_.push_back(FromProto<TOwningKey>(protoKey));
        }

        Misc_ = GetProtoExtension<TMiscExt>(ChunkMeta_.extensions());
        BlockMeta_ = GetProtoExtension<TBlockMetaExt>(ChunkMeta_.extensions());

        return this;
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Error caching meta of chunk %v",
            chunkReader->GetChunkId())
            << ex;
    }
}

void TCachedVersionedChunkMeta::ValidateChunkMeta()
{
    auto type = EChunkType(ChunkMeta_.type());
    if (type != EChunkType::Table) {
        THROW_ERROR_EXCEPTION("Incorrect chunk type: actual %Qlv, expected %Qlv",
            type,
            EChunkType::Table);
    }

    auto formatVersion = ETableChunkFormat(ChunkMeta_.version());
    if (formatVersion != ETableChunkFormat::VersionedSimple) {
        THROW_ERROR_EXCEPTION("Incorrect chunk format version: actual %Qlv, expected: %Qlv",
            formatVersion,
            ETableChunkFormat::VersionedSimple);
    }
}

void TCachedVersionedChunkMeta::ValidateSchema(const TTableSchema& readerSchema)
{
    auto chunkKeyColumnsExt = GetProtoExtension<TKeyColumnsExt>(ChunkMeta_.extensions());
    auto chunkKeyColumns = NYT::FromProto<TKeyColumns>(chunkKeyColumnsExt);   
    if (KeyColumns_ != chunkKeyColumns) {
        THROW_ERROR_EXCEPTION("Incorrect key columns: actual [%v], expected [%v]",
            JoinToString(chunkKeyColumns),
            JoinToString(KeyColumns_));
    }

    auto protoSchema = GetProtoExtension<TTableSchemaExt>(ChunkMeta_.extensions());
    FromProto(&ChunkSchema_, protoSchema);

    SchemaIdMapping_.reserve(readerSchema.Columns().size() - KeyColumns_.size());
    for (int readerIndex = KeyColumns_.size(); readerIndex < readerSchema.Columns().size(); ++readerIndex) {
        auto& column = readerSchema.Columns()[readerIndex];
        auto* chunkColumn = ChunkSchema_.FindColumn(column.Name);
        if (!chunkColumn) {
            // This is a valid case, simply skip the column.
            continue;
        }

        if (chunkColumn->Type != column.Type) {
            THROW_ERROR_EXCEPTION("Incompatible type for column %Qv: actual: %Qlv, expected %Qlv",
                column.Name,
                chunkColumn->Type,
                column.Type);
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
