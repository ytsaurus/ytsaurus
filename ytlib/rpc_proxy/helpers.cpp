#include "helpers.h"

#include <yt/ytlib/api/rowset.h>

#include <yt/ytlib/table_client/unversioned_row.h>
#include <yt/ytlib/table_client/row_base.h>
#include <yt/ytlib/table_client/row_buffer.h>
#include <yt/ytlib/table_client/name_table.h>
#include <yt/ytlib/table_client/schema.h>

#include <yt/ytlib/tablet_client/table_mount_cache.h>
#include <yt/ytlib/tablet_client/wire_protocol.h>

namespace NYT {
namespace NRpcProxy {

////////////////////////////////////////////////////////////////////////////////

using namespace NApi;
using namespace NTableClient;
using namespace NTabletClient;

////////////////////////////////////////////////////////////////////////////////

namespace NProto {

////////////////////////////////////////////////////////////////////////////////
// OPTIONS
////////////////////////////////////////////////////////////////////////////////

void ToProto(
    NProto::TTransactionalOptions* proto,
    const NApi::TTransactionalOptions& options)
{
    if (options.TransactionId) {
        ToProto(proto->mutable_transaction_id(), options.TransactionId);
    }
    proto->set_ping(options.Ping);
    proto->set_ping_ancestors(options.PingAncestors);
    proto->set_sticky(options.Sticky);
}

void ToProto(
    NProto::TPrerequisiteOptions* proto,
    const NApi::TPrerequisiteOptions& options)
{
    for (const auto& item : options.PrerequisiteTransactionIds) {
        auto* protoItem = proto->add_transactions();
        ToProto(protoItem->mutable_transaction_id(), item);
    }
    for (const auto& item : options.PrerequisiteRevisions) {
        auto* protoItem = proto->add_revisions();
        protoItem->set_path(item->Path);
        protoItem->set_revision(item->Revision);
        ToProto(protoItem->mutable_transaction_id(), item->TransactionId);
    }
}

void ToProto(
    NProto::TMasterReadOptions* proto,
    const NApi::TMasterReadOptions& options)
{
    proto->set_read_from(static_cast<NProto::EMasterReadKind>(options.ReadFrom));
    proto->set_success_expiration_time(NYT::ToProto<i64>(options.ExpireAfterSuccessfulUpdateTime));
    proto->set_failure_expiration_time(NYT::ToProto<i64>(options.ExpireAfterFailedUpdateTime));
    proto->set_cache_sticky_group_size(options.CacheStickyGroupSize);
}

void ToProto(
    NProto::TMutatingOptions* proto,
    const NApi::TMutatingOptions& options)
{
    ToProto(proto->mutable_mutation_id(), options.GetOrGenerateMutationId());
    proto->set_retry(options.Retry);
}

void ToProto(
    NProto::TSuppressableAccessTrackingOptions* proto,
    const NApi::TSuppressableAccessTrackingOptions& options)
{
    proto->set_suppress_access_tracking(options.SuppressAccessTracking);
    proto->set_suppress_modification_tracking(options.SuppressModificationTracking);
}

void ToProto(
    NProto::TTabletRangeOptions* proto,
    const NApi::TTabletRangeOptions& options)
{
    if (options.FirstTabletIndex) {
        proto->set_first_tablet_index(*options.FirstTabletIndex);
    }
    if (options.LastTabletIndex) {
        proto->set_last_tablet_index(*options.LastTabletIndex);
    }
}

void ToProto(
    NProto::TTabletReadOptions* proto,
    const NApi::TTabletReadOptions& options)
{
    proto->set_read_from(static_cast<NProto::ETabletReadKind>(options.ReadFrom));
}

////////////////////////////////////////////////////////////////////////////////
// RESULTS
////////////////////////////////////////////////////////////////////////////////

void ToProto(
    NProto::TGetFileFromCacheResult* proto,
    const NApi::TGetFileFromCacheResult& result)
{
    proto->set_path(result.Path);
}

void FromProto(
    NApi::TGetFileFromCacheResult* result,
    const NProto::TGetFileFromCacheResult& proto)
{
    result->Path = proto.path();
}

void ToProto(
    NProto::TPutFileToCacheResult* proto,
    const NApi::TPutFileToCacheResult& result)
{
    proto->set_path(result.Path);
}

void FromProto(
    NApi::TPutFileToCacheResult* result,
    const NProto::TPutFileToCacheResult& proto)
{
    result->Path = proto.path();
}


void ToProto(NProto::TColumnSchema* protoSchema, const NTableClient::TColumnSchema& schema)
{
    protoSchema->set_name(schema.Name());
    protoSchema->set_type(static_cast<int>(schema.GetPhysicalType()));
    protoSchema->set_logical_type(static_cast<int>(schema.LogicalType()));
    if (schema.Lock()) {
        protoSchema->set_lock(*schema.Lock());
    }
    if (schema.Expression()) {
        protoSchema->set_expression(*schema.Expression());
    }
    if (schema.Aggregate()) {
        protoSchema->set_aggregate(*schema.Aggregate());
    }
    if (schema.SortOrder()) {
        protoSchema->set_sort_order(static_cast<int>(*schema.SortOrder()));
    }
    if (schema.Group()) {
        protoSchema->set_group(*schema.Group());
    }
    if (schema.Required()) {
        protoSchema->set_required(schema.Required());
    }
}

void FromProto(NTableClient::TColumnSchema* schema, const NProto::TColumnSchema& protoSchema)
{
    schema->SetName(protoSchema.name());
    if (protoSchema.has_logical_type()) {
        schema->SetLogicalType(CheckedEnumCast<ELogicalValueType>(protoSchema.logical_type()));
        YCHECK(schema->GetPhysicalType() == CheckedEnumCast<EValueType>(protoSchema.type()));
    } else {
        schema->SetLogicalType(GetLogicalType(CheckedEnumCast<EValueType>(protoSchema.type())));
    }
    schema->SetLock(protoSchema.has_lock() ? MakeNullable(protoSchema.lock()) : Null);
    schema->SetExpression(protoSchema.has_expression() ? MakeNullable(protoSchema.expression()) : Null);
    schema->SetAggregate(protoSchema.has_aggregate() ? MakeNullable(protoSchema.aggregate()) : Null);
    schema->SetSortOrder(protoSchema.has_sort_order() ? MakeNullable(ESortOrder(protoSchema.sort_order())) : Null);
    schema->SetGroup(protoSchema.has_group() ? MakeNullable(protoSchema.group()) : Null);
    schema->SetRequired(protoSchema.required());
}

void ToProto(NProto::TTableSchema* protoSchema, const NTableClient::TTableSchema& schema)
{
    using NYT::ToProto;

    ToProto(protoSchema->mutable_columns(), schema.Columns());
    protoSchema->set_strict(schema.GetStrict());
    protoSchema->set_unique_keys(schema.GetUniqueKeys());
}

void FromProto(NTableClient::TTableSchema* schema, const NProto::TTableSchema& protoSchema)
{
    using NYT::FromProto;

    *schema = NTableClient::TTableSchema(
        FromProto<std::vector<NTableClient::TColumnSchema>>(protoSchema.columns()),
        protoSchema.strict(),
        protoSchema.unique_keys());
}

void ToProto(NProto::TTabletInfo* protoTabletInfo, const NTabletClient::TTabletInfo& tabletInfo)
{
    ToProto(protoTabletInfo->mutable_tablet_id(), tabletInfo.TabletId);
    protoTabletInfo->set_mount_revision(tabletInfo.MountRevision);
    protoTabletInfo->set_state(static_cast<i32>(tabletInfo.State));
    ToProto(protoTabletInfo->mutable_pivot_key(), tabletInfo.PivotKey);
    if (tabletInfo.CellId) {
        ToProto(protoTabletInfo->mutable_cell_id(), tabletInfo.CellId);
    }
}

void FromProto(NTabletClient::TTabletInfo* tabletInfo, const NProto::TTabletInfo& protoTabletInfo)
{
    using NYT::FromProto;

    tabletInfo->TabletId =
        FromProto<TTabletId>(protoTabletInfo.tablet_id());
    tabletInfo->MountRevision = protoTabletInfo.mount_revision();
    tabletInfo->State = CheckedEnumCast<ETabletState>(protoTabletInfo.state());
    tabletInfo->PivotKey = FromProto<NTableClient::TOwningKey>(protoTabletInfo.pivot_key());
    if (protoTabletInfo.has_cell_id()) {
        tabletInfo->CellId = FromProto<TTabletCellId>(protoTabletInfo.cell_id());
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NProto

////////////////////////////////////////////////////////////////////////////////

void SetTimeoutOptions(
    NRpc::TClientRequest& request,
    const TTimeoutOptions& options)
{
    request.SetTimeout(options.Timeout);
}

////////////////////////////////////////////////////////////////////////////////
// ROWSETS
////////////////////////////////////////////////////////////////////////////////

template <class TRow>
struct TRowsetTraits;

template <>
struct TRowsetTraits<TUnversionedRow>
{
    static constexpr NProto::ERowsetKind Kind = NProto::RK_UNVERSIONED;
};

template <>
struct TRowsetTraits<TVersionedRow>
{
    static constexpr NProto::ERowsetKind Kind = NProto::RK_VERSIONED;
};

struct TRpcProxyRowsetBufferTag
{ };

void ValidateRowsetDescriptor(
    const NProto::TRowsetDescriptor& descriptor,
    int expectedVersion,
    NProto::ERowsetKind expectedKind)
{
    if (descriptor.wire_format_version() != expectedVersion) {
        THROW_ERROR_EXCEPTION(
            "Incompatible rowset wire format version: expected %v, got %v",
            expectedVersion,
            descriptor.wire_format_version());
    }
    if (descriptor.rowset_kind() != expectedKind) {
        THROW_ERROR_EXCEPTION(
            "Incompatible rowset kind: expected %v, got %v",
            NProto::ERowsetKind_Name(expectedKind),
            NProto::ERowsetKind_Name(descriptor.rowset_kind()));
    }
}

std::vector<TSharedRef> SerializeRowset(
    const NTableClient::TNameTablePtr& nameTable,
    const TRange<NTableClient::TUnversionedRow>& rows,
    NProto::TRowsetDescriptor* descriptor)
{
    descriptor->set_wire_format_version(1);
    descriptor->set_rowset_kind(NProto::RK_UNVERSIONED);
    for (size_t id = 0; id < nameTable->GetSize(); ++id) {
        auto* columnDescriptor = descriptor->add_columns();
        columnDescriptor->set_name(TString(nameTable->GetName(id)));
    }
    TWireProtocolWriter writer;
    writer.WriteUnversionedRowset(rows);
    return writer.Finish();
}

template <class TRow>
std::vector<TSharedRef> SerializeRowset(
    const TTableSchema& schema,
    const TRange<TRow>& rows,
    NProto::TRowsetDescriptor* descriptor)
{
    descriptor->set_wire_format_version(1);
    descriptor->set_rowset_kind(TRowsetTraits<TRow>::Kind);
    for (const auto& column : schema.Columns()) {
        auto* columnDescriptor = descriptor->add_columns();
        columnDescriptor->set_name(column.Name());
        // we save physical type for backward compatibility
        columnDescriptor->set_type(static_cast<int>(column.GetPhysicalType()));
        columnDescriptor->set_logical_type(static_cast<int>(column.LogicalType()));
    }
    TWireProtocolWriter writer;
    writer.WriteRowset(rows);
    return writer.Finish();
}

// Instatiate templates.
template std::vector<TSharedRef> SerializeRowset(
    const TTableSchema& schema,
    const TRange<TUnversionedRow>& rows,
    NProto::TRowsetDescriptor* descriptor);
template std::vector<TSharedRef> SerializeRowset(
    const TTableSchema& schema,
    const TRange<TVersionedRow>& rows,
    NProto::TRowsetDescriptor* descriptor);

TTableSchema DeserializeRowsetSchema(
    const NProto::TRowsetDescriptor& descriptor)
{
    std::vector<TColumnSchema> columns;
    columns.resize(descriptor.columns_size());
    for (int i = 0; i < descriptor.columns_size(); ++i) {
        if (descriptor.columns(i).has_name()) {
            columns[i].SetName(descriptor.columns(i).name());
        }
        if (descriptor.columns(i).has_logical_type()) {
            columns[i].SetLogicalType(static_cast<NTableClient::ELogicalValueType>(descriptor.columns(i).logical_type()));
        } else if (descriptor.columns(i).has_type()) {
            columns[i].SetLogicalType(static_cast<NTableClient::ELogicalValueType>(descriptor.columns(i).type()));
        }
    }
    return TTableSchema(std::move(columns));
}

template <class TRow>
TIntrusivePtr<NApi::IRowset<TRow>> DeserializeRowset(
    const NProto::TRowsetDescriptor& descriptor,
    const TSharedRef& data)
{
    ValidateRowsetDescriptor(descriptor, 1, TRowsetTraits<TRow>::Kind);
    TWireProtocolReader reader(data, New<TRowBuffer>(TRpcProxyRowsetBufferTag()));
    auto schema = DeserializeRowsetSchema(descriptor);
    auto schemaData = TWireProtocolReader::GetSchemaData(schema, TColumnFilter());
    auto rows = reader.ReadRowset<TRow>(schemaData, true);
    return NApi::CreateRowset(std::move(schema), std::move(rows));
}

// Instatiate templates.
template NApi::IUnversionedRowsetPtr DeserializeRowset(
    const NProto::TRowsetDescriptor& descriptor,
    const TSharedRef& data);
template NApi::IVersionedRowsetPtr DeserializeRowset(
    const NProto::TRowsetDescriptor& descriptor,
    const TSharedRef& data);

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpcProxy
} // namespace NYT
