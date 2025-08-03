#include "compact_table_schema.h"

#include <yt/yt/server/master/cell_master/serialize.h>

#include <yt/yt/client/table_client/comparator.h>
#include <yt/yt/client/table_client/schema.h>

namespace NYT::NTableServer {

using namespace NConcurrency;
using namespace NTableClient;

using NYT::FromProto;

///////////////////////////////////////////////////////////////////////////////

static const std::string EmptyWireProtoTableSchema = SerializeToWireProto(TTableSchema());

// NB: Need to ensure that all fields in wire protobuf will be set into correct values.
TCompactTableSchema::TCompactTableSchema()
    : TableSchema_(EmptyWireProtoTableSchema)
{ }

TCompactTableSchema::TCompactTableSchema(const NTableClient::NProto::TTableSchemaExt& schema)
    : TableSchema_(schema.SerializeAsStringOrThrow())
{
    InitializeFromProto(schema);
}

TCompactTableSchema::TCompactTableSchema(const TTableSchema& schema)
    : TableSchema_(SerializeToWireProto(schema))
{
    InitializePartial(
        schema.IsEmpty(),
        schema.IsStrict(),
        schema.IsUniqueKeys(),
        schema.GetSchemaModification());

    for (const auto& column : schema.Columns()) {
        if (column.SortOrder()) {
            KeyColumns_.push_back(column.Name());
            SortOrders_.push_back(*column.SortOrder());
        }
        if (column.MaxInlineHunkSize()) {
            HasHunkColumns_ = true;
        }
    }
}

TCompactTableSchema::TCompactTableSchema(const NTableClient::TTableSchemaPtr& schema)
    : TCompactTableSchema(*schema)
{ }

const std::string& TCompactTableSchema::AsWireProto() const
{
    return TableSchema_;
}

bool TCompactTableSchema::IsSorted() const
{
    return GetKeyColumnCount() > 0;
}

bool TCompactTableSchema::HasHunkColumns() const
{
    return HasHunkColumns_;
}

bool TCompactTableSchema::HasNontrivialSchemaModification() const
{
    return HasNontrivialSchemaModification_;
}

int TCompactTableSchema::GetKeyColumnCount() const
{
    return std::ssize(KeyColumns_);
}

const TKeyColumns& TCompactTableSchema::GetKeyColumns() const
{
    return KeyColumns_;
}

const std::vector<ESortOrder>& TCompactTableSchema::GetSortOrders() const
{
    return SortOrders_;
}

i64 TCompactTableSchema::GetMemoryUsage() const
{
    static constexpr i64 CompactTableSchemaSize = sizeof(TCompactTableSchema);
    // NB: +1 for termination character of each key column.
    i64 memoryUsage = CompactTableSchemaSize + std::ssize(KeyColumns_);
    for (const auto& keyColumn : KeyColumns_) {
        memoryUsage += keyColumn.size();
    }
    memoryUsage += sizeof(ESortOrder) * SortOrders_.size();
    // NB: +1 is for termination character.
    memoryUsage += TableSchema_.size() + 1;
    return memoryUsage;
}

TComparator TCompactTableSchema::ToComparator(TCallback<TUUComparerSignature> cgComparator) const
{
    return TComparator(SortOrders_, std::move(cgComparator));
}

void TCompactTableSchema::Save(NCellMaster::TSaveContext& context) const
{
    using NYT::Save;

    Save(context, TableSchema_);
}

void TCompactTableSchema::Load(NCellMaster::TLoadContext& context)
{
    using NYT::Load;

    Load(context, TableSchema_);
    NTableClient::NProto::TTableSchemaExt protoTableSchema;
    YT_VERIFY(protoTableSchema.ParseFromString(TableSchema_));
    InitializeFromProto(protoTableSchema);
}

void TCompactTableSchema::InitializePartial(
    bool empty,
    bool strict,
    bool uniqueKeys,
    ETableSchemaModification schemaModification)
{
    Empty_ = empty;
    Strict_ = strict;
    UniqueKeys_ = uniqueKeys;
    HasNontrivialSchemaModification_ = (schemaModification != ETableSchemaModification::None);
}

void TCompactTableSchema::InitializeFromProto(const NTableClient::NProto::TTableSchemaExt& protoSchema)
{
    InitializePartial(
        protoSchema.columns().empty(),
        protoSchema.strict(),
        protoSchema.unique_keys(),
        FromProto<ETableSchemaModification>(protoSchema.schema_modification()));

    for (const auto& column : protoSchema.columns()) {
        if (column.has_sort_order()) {
            KeyColumns_.push_back(column.name());
            SortOrders_.push_back(FromProto<ESortOrder>(column.sort_order()));
        }
        if (column.has_max_inline_hunk_size()) {
            HasHunkColumns_ = true;
        }
    }
}

void TCompactTableSchema::SerializeToProto(NTableClient::NProto::TTableSchemaExt* protoSchema) const
{
    THROW_ERROR_EXCEPTION_UNLESS(
        protoSchema->ParseFromString(TableSchema_),
        "Failed to deserialize table schema from wire protobuf");
}

////////////////////////////////////////////////////////////////////////////////

void ToProto(NTableClient::NProto::TTableSchemaExt* protoSchema, const TCompactTableSchema& schema)
{
    protoSchema->Clear();
    schema.SerializeToProto(protoSchema);
}

void ToProto(NTableClient::NProto::TTableSchemaExt* protoSchema, const TCompactTableSchemaPtr& schema)
{
    if (schema) {
        ToProto(protoSchema, *schema);
    } else {
        protoSchema->Clear();
    }
}

void FromProto(TCompactTableSchema* schema, const NTableClient::NProto::TTableSchemaExt& protoSchema)
{
    *schema = TCompactTableSchema(protoSchema);
}

////////////////////////////////////////////////////////////////////////////////

size_t TCompactTableSchemaHash::operator()(const TCompactTableSchema& schema) const
{
    return THash<TCompactTableSchema>()(schema);
}

size_t TCompactTableSchemaHash::operator()(const TCompactTableSchemaPtr& schema) const
{
    return THash<TCompactTableSchema>()(*schema);
}

bool TCompactTableSchemaEquals::operator()(const TCompactTableSchema& lhs, const TCompactTableSchema& rhs) const
{
    return lhs == rhs;
}

bool TCompactTableSchemaEquals::operator()(const TCompactTableSchemaPtr& lhs, const TCompactTableSchemaPtr& rhs) const
{
    return *lhs == *rhs;
}

bool TCompactTableSchemaEquals::operator()(const TCompactTableSchemaPtr& lhs, const TCompactTableSchema& rhs) const
{
    return *lhs == rhs;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableServer

size_t THash<NYT::NTableServer::TCompactTableSchema>::operator()(const NYT::NTableServer::TCompactTableSchema& tableSchema) const
{
    return THash<std::string>()(tableSchema.AsWireProto());
}
