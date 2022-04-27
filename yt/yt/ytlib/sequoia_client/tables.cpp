#include "tables.h"

#include <yt/yt/ytlib/query_client/column_evaluator.h>

#include <yt/yt/client/table_client/comparator.h>
#include <yt/yt/client/table_client/row_base.h>
#include <yt/yt/client/table_client/row_buffer.h>
#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/unversioned_row.h>

namespace NYT::NSequoiaClient {

using namespace NTableClient;
using namespace NQueryClient;

////////////////////////////////////////////////////////////////////////////////

TChunkMetaExtensionsTableDescriptor::TChunkMetaExtensionsTableDescriptor()
    : Schema_(InitSchema())
    , NameTable_(TNameTable::FromSchema(*Schema_))
    , ColumnEvaluator_(TColumnEvaluator::Create(Schema_, /*typeInferrers*/ nullptr, /*profilers*/ nullptr))
    , Index_(NameTable_)
{ }

const TChunkMetaExtensionsTableDescriptorPtr& TChunkMetaExtensionsTableDescriptor::Get()
{
    const static TChunkMetaExtensionsTableDescriptorPtr Table = New<TChunkMetaExtensionsTableDescriptor>();
    return Table;
}

ESequoiaTable TChunkMetaExtensionsTableDescriptor::GetType() const
{
    return ESequoiaTable::ChunkMetaExtensions;
}

const TString& TChunkMetaExtensionsTableDescriptor::GetName() const
{
    const static TString Name = "chunk_meta_extensions";
    return Name;
}

const TTableSchemaPtr& TChunkMetaExtensionsTableDescriptor::GetSchema() const
{
    return Schema_;
}

const TNameTablePtr& TChunkMetaExtensionsTableDescriptor::GetNameTable() const
{
    return NameTable_;
}

const TColumnEvaluatorPtr& TChunkMetaExtensionsTableDescriptor::GetColumnEvaluator() const
{
    return ColumnEvaluator_;
}

const TChunkMetaExtensionsTableDescriptor::TIndex& TChunkMetaExtensionsTableDescriptor::GetIndex() const
{
    return Index_;
}

TUnversionedRow TChunkMetaExtensionsTableDescriptor::ToKey(
    const TChunkMetaExtensionsRow& chunkMetaExtensions,
    const TRowBufferPtr& rowBuffer)
{
    TUnversionedRowBuilder builder;
    builder.AddValue(MakeUnversionedStringValue(chunkMetaExtensions.Id, Index_.Id));

    return rowBuffer->CaptureRow(builder.GetRow());
}

TUnversionedRow TChunkMetaExtensionsTableDescriptor::ToUnversionedRow(
    const TChunkMetaExtensionsRow& chunkMetaExtensions,
    const TRowBufferPtr& rowBuffer)
{
    TUnversionedRowBuilder builder;
    builder.AddValue(MakeUnversionedStringValue(chunkMetaExtensions.Id, Index_.Id));
    builder.AddValue(MakeUnversionedStringValue(chunkMetaExtensions.MiscExt, Index_.MiscExt));
    builder.AddValue(MakeUnversionedStringValue(chunkMetaExtensions.HunkChunkRefsExt, Index_.HunkChunkRefsExt));
    builder.AddValue(MakeUnversionedStringValue(chunkMetaExtensions.HunkChunkMiscExt, Index_.HunkChunkMiscExt));
    builder.AddValue(MakeUnversionedStringValue(chunkMetaExtensions.BoundaryKeysExt, Index_.BoundaryKeysExt));
    builder.AddValue(MakeUnversionedStringValue(chunkMetaExtensions.HeavyColumnStatisticsExt, Index_.HeavyColumnStatisticsExt));

    return rowBuffer->CaptureRow(builder.GetRow());
}

TChunkMetaExtensionsTableDescriptor::TChunkMetaExtensionsRow TChunkMetaExtensionsTableDescriptor::FromUnversionedRow(
    TUnversionedRow row)
{
    TChunkMetaExtensionsRow chunkMetaExtensions;
    for (auto value : row) {
        if (value.Id == Index_.Id) {
            chunkMetaExtensions.Id = value.AsString();
        } else if (value.Id == Index_.MiscExt) {
            chunkMetaExtensions.MiscExt = value.AsString();
        } else if (value.Id == Index_.HunkChunkRefsExt) {
            chunkMetaExtensions.HunkChunkRefsExt = value.AsString();
        } else if (value.Id == Index_.HunkChunkMiscExt) {
            chunkMetaExtensions.HunkChunkMiscExt = value.AsString();
        } else if (value.Id == Index_.BoundaryKeysExt) {
            chunkMetaExtensions.BoundaryKeysExt = value.AsString();
        } else if (value.Id == Index_.HeavyColumnStatisticsExt) {
            chunkMetaExtensions.HeavyColumnStatisticsExt = value.AsString();
        } else {
            YT_ABORT();
        }
    }

    return chunkMetaExtensions;
}

TTableSchemaPtr TChunkMetaExtensionsTableDescriptor::InitSchema()
{
    std::vector<TColumnSchema> columns({
        TColumnSchema("id", EValueType::String, ESortOrder::Ascending),
        TColumnSchema("misc_ext", EValueType::String),
        TColumnSchema("hunk_chunk_refs_ext", EValueType::String),
        TColumnSchema("hunk_chunk_misc_ext", EValueType::String),
        TColumnSchema("boundary_keys_ext", EValueType::String),
        TColumnSchema("heavy_column_statistics_ext", EValueType::String),
    });

    return New<TTableSchema>(std::move(columns));
}

TChunkMetaExtensionsTableDescriptor::TIndex::TIndex(const TNameTablePtr& nameTable)
    : Id(nameTable->GetId("id"))
    , MiscExt(nameTable->GetId("misc_ext"))
    , HunkChunkRefsExt(nameTable->GetId("hunk_chunk_refs_ext"))
    , HunkChunkMiscExt(nameTable->GetId("hunk_chunk_misc_ext"))
    , BoundaryKeysExt(nameTable->GetId("boundary_keys_ext"))
    , HeavyColumnStatisticsExt(nameTable->GetId("heavy_column_statistics_ext"))
{ }

////////////////////////////////////////////////////////////////////////////////

ITableDescriptorPtr GetTableDescriptor(ESequoiaTable table)
{
    switch (table) {
        case ESequoiaTable::ChunkMetaExtensions:
            return TChunkMetaExtensionsTableDescriptor::Get();
        default:
            YT_ABORT();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSequoiaClient
