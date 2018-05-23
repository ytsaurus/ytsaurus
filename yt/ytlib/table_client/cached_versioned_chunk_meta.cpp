#include "cached_versioned_chunk_meta.h"
#include "schema.h"
#include "name_table.h"

#include <yt/ytlib/misc/workload.h>

#include <yt/ytlib/chunk_client/chunk_reader.h>
#include <yt/ytlib/chunk_client/dispatcher.h>
#include <yt/ytlib/chunk_client/chunk_reader_statistics.h>

#include <yt/core/ytree/convert.h>

#include <yt/core/concurrency/scheduler.h>

#include <yt/core/misc/bloom_filter.h>

namespace NYT {
namespace NTableClient {

using namespace NConcurrency;
using namespace NTableClient::NProto;
using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NNodeTrackerClient;
using namespace NYson;
using namespace NYTree;

using NYT::FromProto;
using NChunkClient::TChunkReaderStatistics;

////////////////////////////////////////////////////////////////////////////////

TCachedVersionedChunkMeta::TCachedVersionedChunkMeta() = default;

TCachedVersionedChunkMetaPtr TCachedVersionedChunkMeta::Create(
    const TChunkId& chunkId,
    const NChunkClient::NProto::TChunkMeta& chunkMeta,
    const TTableSchema& schema,
    TNodeMemoryTracker* memoryTracker)
{
    try {
        auto cachedMeta = New<TCachedVersionedChunkMeta>();
        cachedMeta->Init(chunkId, chunkMeta, schema, memoryTracker);
        return cachedMeta;
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Error caching meta of chunk %v",
            chunkId)
            << ex;
    }
}

TFuture<TCachedVersionedChunkMetaPtr> TCachedVersionedChunkMeta::Load(
    IChunkReaderPtr chunkReader,
    const TClientBlockReadOptions& blockReadOptions,
    const TTableSchema& schema,
    TNodeMemoryTracker* memoryTracker)
{
    auto chunkId = chunkReader->GetChunkId();
    return chunkReader->GetMeta(blockReadOptions)
        .Apply(BIND([=] (const NChunkClient::NProto::TChunkMeta& chunkMeta) {
            return TCachedVersionedChunkMeta::Create(chunkId, chunkMeta, schema, memoryTracker);
        }));
}

void TCachedVersionedChunkMeta::Init(
    const TChunkId& chunkId,
    const NChunkClient::NProto::TChunkMeta& chunkMeta,
    const TTableSchema& schema,
    TNodeMemoryTracker* memoryTracker)
{
    ChunkId_ = chunkId;

    auto keyColumns = schema.GetKeyColumns();
    KeyColumnCount_ = keyColumns.size();

    TColumnarChunkMeta::InitExtensions(chunkMeta);
    TColumnarChunkMeta::InitBlockLastKeys(keyColumns);

    ValidateChunkMeta();
    ValidateSchema(schema);

    Schema_ = schema;

    auto boundaryKeysExt = FindProtoExtension<TBoundaryKeysExt>(chunkMeta.extensions());
    if (boundaryKeysExt) {
        MinKey_ = WidenKey(FromProto<TOwningKey>(boundaryKeysExt->min()), GetKeyColumnCount());
        MaxKey_ = WidenKey(FromProto<TOwningKey>(boundaryKeysExt->max()), GetKeyColumnCount());
    }

    if (memoryTracker) {
        MemoryTrackerGuard_ = TNodeMemoryTrackerGuard::Acquire(
            memoryTracker,
            EMemoryCategory::CachedVersionedChunkMeta,
            GetMemoryUsage());
    }
}

void TCachedVersionedChunkMeta::ValidateChunkMeta()
{
    if (ChunkType_ != EChunkType::Table) {
        THROW_ERROR_EXCEPTION("Incorrect chunk type: actual %Qlv, expected %Qlv",
            ChunkType_,
            EChunkType::Table);
    }

    if (ChunkFormat_ != ETableChunkFormat::VersionedSimple &&
        ChunkFormat_ != ETableChunkFormat::VersionedColumnar &&
        ChunkFormat_ != ETableChunkFormat::UnversionedColumnar &&
        ChunkFormat_ != ETableChunkFormat::SchemalessHorizontal)
    {
        THROW_ERROR_EXCEPTION("Incorrect chunk format %Qlv",
            ChunkFormat_);
    }
}

void TCachedVersionedChunkMeta::ValidateSchema(const TTableSchema& readerSchema)
{
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
        YCHECK (column.SortOrder());

        if (readerIndex < ChunkSchema_.GetKeyColumnCount()) {
            const auto& chunkColumn = ChunkSchema_.Columns()[readerIndex];
            YCHECK(chunkColumn.SortOrder());

            if (chunkColumn.Name() != column.Name() ||
                chunkColumn.GetPhysicalType() != column.GetPhysicalType() ||
                chunkColumn.SortOrder() != column.SortOrder())
            {
                throwIncompatibleKeyColumns();
            }
        } else {
            auto* chunkColumn = ChunkSchema_.FindColumn(column.Name());
            if (chunkColumn) {
                THROW_ERROR_EXCEPTION(
                    "Incompatible reader key columns: %Qv is a non-key column in chunk schema %v",
                    column.Name(),
                    ConvertToYsonString(ChunkSchema_, EYsonFormat::Text).GetData());
            }
        }
    }

    for (int readerIndex = readerSchema.GetKeyColumnCount(); readerIndex < readerSchema.Columns().size(); ++readerIndex) {
        auto& column = readerSchema.Columns()[readerIndex];
        auto* chunkColumn = ChunkSchema_.FindColumn(column.Name());
        if (!chunkColumn) {
            // This is a valid case, simply skip the column.
            continue;
        }

        if (chunkColumn->GetPhysicalType() != column.GetPhysicalType()) {
            THROW_ERROR_EXCEPTION(
                "Incompatible type %Qlv for column %Qv in chunk schema %v",
                column.GetPhysicalType(),
                column.Name(),
                ConvertToYsonString(ChunkSchema_, EYsonFormat::Text).GetData());
        }

        TColumnIdMapping mapping;
        mapping.ChunkSchemaIndex = ChunkSchema_.GetColumnIndex(*chunkColumn);
        mapping.ReaderSchemaIndex = readerIndex;
        SchemaIdMapping_.push_back(mapping);
    }
}

i64 TCachedVersionedChunkMeta::GetMemoryUsage() const
{
    return TColumnarChunkMeta::GetMemoryUsage() +
        Schema_.GetMemoryUsage();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
