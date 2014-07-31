#include "stdafx.h"
#include "cached_versioned_chunk_meta.h"
#include "schema.h"

#include <ytlib/chunk_client/reader.h>
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

////////////////////////////////////////////////////////////////////////////////

TFuture<TErrorOr<TCachedVersionedChunkMetaPtr>> TCachedVersionedChunkMeta::Load(
    IReaderPtr chunkReader,
    const TTableSchema& schema,
    const TKeyColumns& keyColumns)
{
    auto cachedMeta = New<TCachedVersionedChunkMeta>();

    return BIND(&TCachedVersionedChunkMeta::DoLoad, cachedMeta)
        .AsyncVia(TDispatcher::Get()->GetReaderInvoker())
        .Run(chunkReader, schema, keyColumns);
}

TErrorOr<TCachedVersionedChunkMetaPtr> TCachedVersionedChunkMeta::DoLoad(
    IReaderPtr chunkReader,
    const TTableSchema& readerSchema,
    const TKeyColumns& keyColumns)
{
    try {
        KeyColumns_ = keyColumns;

        {
            auto error = readerSchema.CheckKeyColumns(keyColumns);
            THROW_ERROR_EXCEPTION_IF_FAILED(error);
        }

        auto chunkMetaOrError = WaitFor(chunkReader->GetMeta());
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
        return TError("Error caching meta of chunk %v",
            chunkReader->GetChunkId())
            << ex;
    }
}

void TCachedVersionedChunkMeta::ValidateChunkMeta()
{
    if (ChunkMeta_.type() != EChunkType::Table) {
        THROW_ERROR_EXCEPTION("Incorrect chunk type: actual %Qv, expected %Qv",
            EChunkType(ChunkMeta_.type()),
            EChunkType(EChunkType::Table));
    }

    if (ChunkMeta_.version() != ETableChunkFormat::VersionedSimple) {
        THROW_ERROR_EXCEPTION("Incorrect chunk format version: actual %Qv, expected: %Qv",
            ETableChunkFormat(ChunkMeta_.version()),
            ETableChunkFormat(ETableChunkFormat::VersionedSimple));
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
            THROW_ERROR_EXCEPTION("Incompatible type for column %Qv: actual: %Qv, expected %Qv",
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
