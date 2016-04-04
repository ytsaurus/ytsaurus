#include "cached_versioned_chunk_meta.h"
#include "schema.h"

#include <yt/ytlib/misc/workload.h>

#include <yt/ytlib/chunk_client/chunk_reader.h>
#include <yt/ytlib/chunk_client/dispatcher.h>

#include <yt/core/ytree/convert.h>

#include <yt/core/concurrency/scheduler.h>

#include <yt/core/misc/bloom_filter.h>

namespace NYT {
namespace NTableClient {

using namespace NConcurrency;
using namespace NTableClient::NProto;
using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NYson;
using namespace NYTree;

using NYT::FromProto;

////////////////////////////////////////////////////////////////////////////////

TCachedVersionedChunkMeta::TCachedVersionedChunkMeta() = default;

TCachedVersionedChunkMetaPtr TCachedVersionedChunkMeta::Create(
    const TChunkId& chunkId,
    const NChunkClient::NProto::TChunkMeta& chunkMeta,
    const TTableSchema& schema)
{
    auto cachedMeta = New<TCachedVersionedChunkMeta>();
    try {
        cachedMeta->Init(chunkId, chunkMeta, schema);
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Error caching meta of chunk %v",
            chunkId)
            << ex;
    }
    return cachedMeta;
}

TFuture<TCachedVersionedChunkMetaPtr> TCachedVersionedChunkMeta::Load(
    IChunkReaderPtr chunkReader,
    const TWorkloadDescriptor& workloadDescriptor,
    const TTableSchema& schema)
{
    auto cachedMeta = New<TCachedVersionedChunkMeta>();
    return BIND(&TCachedVersionedChunkMeta::DoLoad, cachedMeta)
        .AsyncVia(TDispatcher::Get()->GetReaderInvoker())
        .Run(chunkReader, workloadDescriptor, schema);
}

TCachedVersionedChunkMetaPtr TCachedVersionedChunkMeta::DoLoad(
    IChunkReaderPtr chunkReader,
    const TWorkloadDescriptor& workloadDescriptor,
    const TTableSchema& schema)
{
    try {
        auto asyncChunkMeta = chunkReader->GetMeta(workloadDescriptor);
        auto chunkMeta = WaitFor(asyncChunkMeta)
            .ValueOrThrow();

        Init(chunkReader->GetChunkId(), chunkMeta, schema);
        return this;
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Error caching meta of chunk %v",
            chunkReader->GetChunkId())
            << ex;
    }
}

void TCachedVersionedChunkMeta::Init(
    const TChunkId& chunkId,
    const NChunkClient::NProto::TChunkMeta& chunkMeta,
    const TTableSchema& schema)
{
    ChunkId_ = chunkId;
    ChunkMeta_ = chunkMeta;

    auto keyColumns = schema.GetKeyColumns();
    KeyColumnCount_ = keyColumns.size();

    ValidateChunkMeta();
    ValidateSchema(schema);

    auto boundaryKeysExt = GetProtoExtension<TBoundaryKeysExt>(ChunkMeta_.extensions());
    MinKey_ = WidenKey(FromProto<TOwningKey>(boundaryKeysExt.min()), GetKeyColumnCount());
    MaxKey_ = WidenKey(FromProto<TOwningKey>(boundaryKeysExt.max()), GetKeyColumnCount());

    Misc_ = GetProtoExtension<TMiscExt>(ChunkMeta_.extensions());
    BlockMeta_ = GetProtoExtension<TBlockMetaExt>(ChunkMeta_.extensions());

    auto columnMeta = FindProtoExtension<TColumnMetaExt>(ChunkMeta_.extensions());
    if (columnMeta) {
        ColumnMeta_.Swap(&*columnMeta);
    }

    BlockLastKeys_.reserve(BlockMeta_.blocks_size());
    for (const auto& block : BlockMeta_.blocks()) {
        YCHECK(block.has_last_key());
        auto key = FromProto<TOwningKey>(block.last_key());
        BlockLastKeys_.push_back(WidenKey(key, GetKeyColumnCount()));
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
    if (formatVersion != ETableChunkFormat::VersionedSimple &&
        formatVersion != ETableChunkFormat::VersionedColumnar)
    {
        THROW_ERROR_EXCEPTION("Incorrect chunk format version: actual %Qlv, expected %Qlv or %Qlv",
            formatVersion,
            ETableChunkFormat::VersionedSimple,
            ETableChunkFormat::VersionedColumnar);
    }
}

void TCachedVersionedChunkMeta::ValidateSchema(const TTableSchema& readerSchema)
{
    auto maybeKeyColumnsExt = FindProtoExtension<TKeyColumnsExt>(ChunkMeta_.extensions());
    auto tableSchemaExt = GetProtoExtension<TTableSchemaExt>(ChunkMeta_.extensions());
    if (maybeKeyColumnsExt) {
        FromProto(&ChunkSchema_, tableSchemaExt, *maybeKeyColumnsExt);
    } else {
        FromProto(&ChunkSchema_, tableSchemaExt);
    }

    ChunkKeyColumnCount_ = ChunkSchema_.GetKeyColumnCount();

    auto throwIncompatibleKeyColumns = [&] () {
        THROW_ERROR_EXCEPTION(
            "Reader key columns %v are incompatible with chunk key columns %v",
            readerSchema.GetKeyColumns(),
            ChunkSchema_.GetKeyColumns());
    };

    if (readerSchema.GetKeyColumnCount() < ChunkSchema_.GetKeyColumnCount()) {
        throwIncompatibleKeyColumns();
    }

    for (int readerIndex = 0; readerIndex < readerSchema.GetKeyColumnCount(); ++readerIndex) {
        auto& column = readerSchema.Columns()[readerIndex];
        YCHECK (column.SortOrder);

        if (readerIndex < ChunkSchema_.GetKeyColumnCount()) {
            const auto& chunkColumn = ChunkSchema_.Columns()[readerIndex];
            YCHECK(chunkColumn.SortOrder);

            if (chunkColumn.Name != column.Name ||
                chunkColumn.Type != column.Type ||
                chunkColumn.SortOrder != column.SortOrder)
            {
                throwIncompatibleKeyColumns();
            }
        } else {
            auto* chunkColumn = ChunkSchema_.FindColumn(column.Name);
            if (chunkColumn) {
                THROW_ERROR_EXCEPTION(
                    "Incompatible reader key columns: %Qv is a non-key column in chunk schema %v",
                    column.Name,
                    ConvertToYsonString(ChunkSchema_, EYsonFormat::Text).Data());
            }
        }
    }

    for (int readerIndex = readerSchema.GetKeyColumnCount(); readerIndex < readerSchema.Columns().size(); ++readerIndex) {
        auto& column = readerSchema.Columns()[readerIndex];
        auto* chunkColumn = ChunkSchema_.FindColumn(column.Name);
        if (!chunkColumn) {
            // This is a valid case, simply skip the column.
            continue;
        }

        if (chunkColumn->Type != column.Type) {
            THROW_ERROR_EXCEPTION(
                "Incompatible type %Qlv for column %Qv in chunk schema %v",
                column.Type,
                column.Name,
                ConvertToYsonString(ChunkSchema_, EYsonFormat::Text).Data());
        }

        TColumnIdMapping mapping;
        mapping.ChunkSchemaIndex = ChunkSchema_.GetColumnIndex(*chunkColumn);
        mapping.ReaderSchemaIndex = readerIndex;
        SchemaIdMapping_.push_back(mapping);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
