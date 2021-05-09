#include "cached_versioned_chunk_meta.h"

#include <yt/yt/client/misc/workload.h>

#include <yt/yt/ytlib/chunk_client/chunk_reader.h>
#include <yt/yt/ytlib/chunk_client/dispatcher.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader_statistics.h>

#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/name_table.h>

#include <yt/yt/core/ytree/convert.h>

#include <yt/yt/core/concurrency/scheduler.h>

#include <yt/yt/core/misc/bloom_filter.h>

namespace NYT::NTableClient {

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
    TChunkId chunkId,
    const NChunkClient::NProto::TChunkMeta& chunkMeta,
    const TTableSchemaPtr& schema,
    const TColumnRenameDescriptors& renameDescriptors,
    const IMemoryUsageTrackerPtr& memoryTracker)
{
    try {
        auto cachedMeta = New<TCachedVersionedChunkMeta>();
        cachedMeta->Init(chunkId, chunkMeta, schema, renameDescriptors, memoryTracker);
        return cachedMeta;
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Error caching meta of chunk %v",
            chunkId)
            << ex;
    }
}

TFuture<TCachedVersionedChunkMetaPtr> TCachedVersionedChunkMeta::Load(
    const IChunkReaderPtr& chunkReader,
    const TClientChunkReadOptions& chunkReadOptions,
    const TTableSchemaPtr& schema,
    const TColumnRenameDescriptors& renameDescriptors,
    const IMemoryUsageTrackerPtr& memoryTracker)
{
    auto chunkId = chunkReader->GetChunkId();
    return chunkReader->GetMeta(chunkReadOptions)
        .Apply(BIND([=] (const TRefCountedChunkMetaPtr& chunkMeta) {
            return TCachedVersionedChunkMeta::Create(chunkId, *chunkMeta, schema, renameDescriptors, memoryTracker);
        }));
}

void TCachedVersionedChunkMeta::Init(
    TChunkId chunkId,
    const NChunkClient::NProto::TChunkMeta& chunkMeta,
    const TTableSchemaPtr& schema,
    const TColumnRenameDescriptors& renameDescriptors,
    const IMemoryUsageTrackerPtr& memoryTracker)
{
    ChunkId_ = chunkId;

    auto keyColumns = schema->GetKeyColumns();
    KeyColumnCount_ = keyColumns.size();

    TColumnarChunkMeta::InitExtensions(chunkMeta);
    TColumnarChunkMeta::RenameColumns(renameDescriptors);
    TColumnarChunkMeta::InitBlockLastKeys(keyColumns);

    ValidateChunkMeta();
    ValidateSchema(*schema);

    Schema_ = schema;

    auto boundaryKeysExt = FindProtoExtension<TBoundaryKeysExt>(chunkMeta.extensions());
    if (boundaryKeysExt) {
        MinKey_ = WidenKey(FromProto<TLegacyOwningKey>(boundaryKeysExt->min()), GetKeyColumnCount());
        MaxKey_ = WidenKey(FromProto<TLegacyOwningKey>(boundaryKeysExt->max()), GetKeyColumnCount());
    }

    if (auto optionalHunkChunkRefsExt = FindProtoExtension<THunkChunkRefsExt>(chunkMeta.extensions())) {
        HunkChunkRefsExt_ = std::move(*optionalHunkChunkRefsExt);
    }

    if (memoryTracker) {
        MemoryTrackerGuard_ = TMemoryUsageTrackerGuard::Acquire(
            memoryTracker,
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
    ChunkKeyColumnCount_ = ChunkSchema_->GetKeyColumnCount();
    auto throwIncompatibleKeyColumns = [&] () {
        THROW_ERROR_EXCEPTION(
            "Reader key columns %v are incompatible with chunk key columns %v",
            readerSchema.GetKeyColumns(),
            ChunkSchema_->GetKeyColumns());
    };

    if (readerSchema.GetKeyColumnCount() < ChunkSchema_->GetKeyColumnCount()) {
        throwIncompatibleKeyColumns();
    }

    for (int readerIndex = 0; readerIndex < readerSchema.GetKeyColumnCount(); ++readerIndex) {
        auto& column = readerSchema.Columns()[readerIndex];
        YT_VERIFY (column.SortOrder());

        if (readerIndex < ChunkSchema_->GetKeyColumnCount()) {
            const auto& chunkColumn = ChunkSchema_->Columns()[readerIndex];
            YT_VERIFY(chunkColumn.SortOrder());

            if (chunkColumn.Name() != column.Name() ||
                chunkColumn.GetPhysicalType() != column.GetPhysicalType() ||
                chunkColumn.SortOrder() != column.SortOrder())
            {
                throwIncompatibleKeyColumns();
            }
        } else {
            auto* chunkColumn = ChunkSchema_->FindColumn(column.Name());
            if (chunkColumn) {
                THROW_ERROR_EXCEPTION(
                    "Incompatible reader key columns: %Qv is a non-key column in chunk schema %v",
                    column.Name(),
                    ConvertToYsonString(ChunkSchema_, EYsonFormat::Text).AsStringBuf());
            }
        }
    }

    for (int readerIndex = readerSchema.GetKeyColumnCount(); readerIndex < std::ssize(readerSchema.Columns()); ++readerIndex) {
        auto& column = readerSchema.Columns()[readerIndex];
        auto* chunkColumn = ChunkSchema_->FindColumn(column.Name());
        if (!chunkColumn) {
            // This is a valid case, simply skip the column.
            continue;
        }

        if (chunkColumn->GetPhysicalType() != column.GetPhysicalType()) {
            THROW_ERROR_EXCEPTION(
                "Incompatible type %Qlv for column %Qv in chunk schema %v",
                column.GetPhysicalType(),
                column.Name(),
                ConvertToYsonString(ChunkSchema_, EYsonFormat::Text).AsStringBuf());
        }

        TColumnIdMapping mapping;
        mapping.ChunkSchemaIndex = ChunkSchema_->GetColumnIndex(*chunkColumn);
        mapping.ReaderSchemaIndex = readerIndex;
        SchemaIdMapping_.push_back(mapping);
    }
}

i64 TCachedVersionedChunkMeta::GetMemoryUsage() const
{
    return
        TColumnarChunkMeta::GetMemoryUsage() +
        Schema_->GetMemoryUsage();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
