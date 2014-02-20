#include "stdafx.h"
#include "cached_versioned_chunk_meta.h"

#include <ytlib/chunk_client/async_reader.h>
#include <ytlib/chunk_client/dispatcher.h>

#include <core/concurrency/fiber.h>

#include <core/misc/string.h>

namespace NYT {
namespace NVersionedTableClient {

using namespace NConcurrency;
using namespace NVersionedTableClient::NProto;
using namespace NChunkClient;
using namespace NChunkClient::NProto;

////////////////////////////////////////////////////////////////////////////////

TCachedVersionedChunkMeta::TCachedVersionedChunkMeta(
    IAsyncReaderPtr asyncReader,
    const TTableSchema& schema,
    const TKeyColumns& keyColumns)
    : KeyColumns_(keyColumns)
    , AsyncReader_(asyncReader)
    , ReaderSchema_(schema)
{ }

TAsyncError TCachedVersionedChunkMeta::Load()
{
    TAsyncError asyncError;

    auto error = ReaderSchema_.CheckKeyColumns(KeyColumns_);
    if (!error.IsOK()) {
        asyncError = MakeFuture(error);
    } else {
        asyncError = BIND(&TCachedVersionedChunkMeta::DoLoad, MakeStrong(this))
            .AsyncVia(TDispatcher::Get()->GetReaderInvoker())
            .Run();
    }

    asyncError.Subscribe(BIND(
        &TCachedVersionedChunkMeta::ReleaseReader,
        MakeWeak(this)));

    return asyncError;
}

TError TCachedVersionedChunkMeta::ValidateSchema()
{
    auto keyColumns = GetProtoExtension<TKeyColumnsExt>(ChunkMeta_.extensions());
    if (keyColumns.names_size() != KeyColumns_.size()) {
        auto protoColumns = NYT::FromProto<Stroka>(keyColumns.names());
        return TError("Incorrect key columns: actual [%s], expected [%s]",
            ~JoinToString(KeyColumns_),
            ~JoinToString(protoColumns));
    }

    if (!std::equal(
        KeyColumns_.begin(),
        KeyColumns_.end(),
        keyColumns.names().begin()))
    {
        auto protoColumns = NYT::FromProto<Stroka>(keyColumns.names());
        return TError("Incorrect key columns: actual [%s], expected [%s]",
            ~JoinToString(KeyColumns_),
            ~JoinToString(protoColumns));
    }

    auto protoSchema = GetProtoExtension<TTableSchemaExt>(ChunkMeta_.extensions());
    FromProto(&ChunkSchema_, protoSchema);

    SchemaIdMapping_.reserve(ReaderSchema_.Columns().size() - KeyColumns_.size());
    for (int readerIndex = KeyColumns_.size(); readerIndex < ReaderSchema_.Columns().size(); ++readerIndex) {
        auto& column = ReaderSchema_.Columns()[readerIndex];
        auto* chunkColumn = ChunkSchema_.FindColumn(column.Name);
        if (!chunkColumn) {
            // ToDo (psushin): this may be valid behavior, just skip the column.
            return TError(
                "Incompatible schema: column %s is absent in chunk schema",
                ~column.Name.Quote());
        }

        if (chunkColumn->Type != column.Type) {
            return TError("Incompatible type for column %s: actual: %s, expected %s",
                ~FormatEnum(chunkColumn->Type).Quote(),
                ~FormatEnum(column.Type).Quote());
        }

        TColumnIdMapping mapping;
        mapping.ChunkSchemaIndex = ChunkSchema_.GetColumnIndex(*chunkColumn);
        mapping.ReaderSchemaIndex = readerIndex;
        SchemaIdMapping_.push_back(mapping);
    }

    return TError();
}

TError TCachedVersionedChunkMeta::DoLoad()
{
    auto getMetaResult = WaitFor(AsyncReader_->AsyncGetChunkMeta());
    RETURN_IF_ERROR(getMetaResult)

    ChunkMeta_ = getMetaResult.GetValue();
    if (ChunkMeta_.type() != EChunkType::Table) {
        return TError("Incorrect chunk type: actual %s, expected %s",
            ~FormatEnum(EChunkType(ChunkMeta_.type())).Quote(),
            ~FormatEnum(EChunkType(EChunkType::Table)).Quote());
    }

    if (ChunkMeta_.version() != ETableChunkFormat::SimpleVersioned) {
        return TError("Incorrect chunk format version: actual %s, expected: %s",
            ~FormatEnum(ETableChunkFormat(ChunkMeta_.version())).Quote(),
            ~FormatEnum(ETableChunkFormat(ETableChunkFormat::SimpleVersioned)).Quote());
    }

    auto error = ValidateSchema();
    RETURN_IF_ERROR(error)

    Misc_ = GetProtoExtension<TMiscExt>(ChunkMeta_.extensions());
    BlockMeta_ = GetProtoExtension<TBlockMetaExt>(ChunkMeta_.extensions());
    BlockIndex_ = GetProtoExtension<TBlockIndexExt>(ChunkMeta_.extensions());
    BoundaryKeys_ = GetProtoExtension<TBoundaryKeysExt>(ChunkMeta_.extensions());

    return TError();
}

void TCachedVersionedChunkMeta::ReleaseReader(TError /* error */)
{
    AsyncReader_.Reset();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NVersionedTableClient
} // namespace NYT
