#include "tables.h"

#include <yt/yt/ytlib/chunk_client/chunk_meta_extensions.h>

#include <yt/yt/ytlib/table_client/chunk_meta_extensions.h>

#include <yt/yt/ytlib/query_client/column_evaluator.h>

#include <yt/yt/client/table_client/comparator.h>
#include <yt/yt/client/table_client/row_base.h>
#include <yt/yt/client/table_client/row_buffer.h>
#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/unversioned_row.h>

#include <yt/yt/core/misc/protobuf_helpers.h>

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
    TUnversionedRow row,
    const TNameTablePtr& nameTable)
{
    TChunkMetaExtensionsRow chunkMetaExtensions;
    for (auto value : row) {
        if (value.Id == nameTable->FindId("id")) {
            chunkMetaExtensions.Id = value.AsString();
        } else if (value.Id == nameTable->FindId("misc_ext")) {
            chunkMetaExtensions.MiscExt = value.AsString();
        } else if (value.Id == nameTable->FindId("hunk_chunk_refs_ext")) {
            chunkMetaExtensions.HunkChunkRefsExt = value.AsString();
        } else if (value.Id == nameTable->FindId("hunk_chunk_misc_ext")) {
            chunkMetaExtensions.HunkChunkMiscExt = value.AsString();
        } else if (value.Id == nameTable->FindId("boundary_keys_ext")) {
            chunkMetaExtensions.BoundaryKeysExt = value.AsString();
        } else if (value.Id == nameTable->FindId("heavy_column_statistics_ext")) {
            chunkMetaExtensions.HeavyColumnStatisticsExt = value.AsString();
        } else {
            YT_ABORT();
        }
    }

    return chunkMetaExtensions;
}

int TChunkMetaExtensionsTableDescriptor::GetColumnId(int extensionTag)
{
    const auto& index = GetIndex();
    switch (extensionTag) {
        case TProtoExtensionTag<NChunkClient::NProto::TMiscExt>::Value:
            return index.MiscExt;
        case TProtoExtensionTag<NTableClient::NProto::THunkChunkRefsExt>::Value:
            return index.HunkChunkRefsExt;
        case TProtoExtensionTag<NTableClient::NProto::THunkChunkMiscExt>::Value:
            return index.HunkChunkMiscExt;
        case TProtoExtensionTag<NTableClient::NProto::TBoundaryKeysExt>::Value:
            return index.BoundaryKeysExt;
        case TProtoExtensionTag<NTableClient::NProto::THeavyColumnStatisticsExt>::Value:
            return index.HeavyColumnStatisticsExt;
        default:
            YT_ABORT();
    }
}

TColumnFilter TChunkMetaExtensionsTableDescriptor::GetColumnFilter(const THashSet<int>& extensionTags)
{
    std::vector<int> columnIds;
    columnIds.reserve(extensionTags.size());
    for (auto extensionTag : extensionTags) {
        columnIds.push_back(GetColumnId(extensionTag));
    }

    return TColumnFilter(std::move(columnIds));
}

void TChunkMetaExtensionsTableDescriptor::SetMetaExtensions(
    const TChunkMetaExtensionsRow& row,
    NYT::NProto::TExtensionSet* extensions)
{
    auto addExtension = [&] (
        int tag,
        const TString& value)
    {
        if (!value) {
            return;
        }

        auto* protoExtension = extensions->add_extensions();
        protoExtension->set_tag(tag);
        protoExtension->set_data(value);
    };
    addExtension(TProtoExtensionTag<NChunkClient::NProto::TMiscExt>::Value, row.MiscExt);
    addExtension(TProtoExtensionTag<NTableClient::NProto::THunkChunkRefsExt>::Value, row.HunkChunkRefsExt);
    addExtension(TProtoExtensionTag<NTableClient::NProto::THunkChunkMiscExt>::Value, row.HunkChunkMiscExt);
    addExtension(TProtoExtensionTag<NTableClient::NProto::TBoundaryKeysExt>::Value, row.BoundaryKeysExt);
    addExtension(TProtoExtensionTag<NTableClient::NProto::THeavyColumnStatisticsExt>::Value, row.HeavyColumnStatisticsExt);
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
