#include "helpers.h"

#include <yt/ytlib/api/client.h>
#include <yt/ytlib/api/rowset.h>

#include <yt/ytlib/table_client/unversioned_row.h>
#include <yt/ytlib/table_client/row_base.h>
#include <yt/ytlib/table_client/row_buffer.h>
#include <yt/ytlib/table_client/name_table.h>
#include <yt/ytlib/table_client/schema.h>

#include <yt/ytlib/tablet_client/wire_protocol.h>

namespace NYT {
namespace NRpcProxy {

////////////////////////////////////////////////////////////////////////////////

using namespace NApi;
using namespace NTableClient;
using namespace NTabletClient;

////////////////////////////////////////////////////////////////////////////////

////////////////////////////////////////////////////////////////////////////////
// OPTIONS
////////////////////////////////////////////////////////////////////////////////

void SetTimeoutOptions(
    NRpc::TClientRequest& proto,
    const TTimeoutOptions& options)
{
    proto.SetTimeout(options.Timeout);
}

void ToProto(
    NProto::TTransactionalOptions* proto,
    const TTransactionalOptions& options)
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
    const TPrerequisiteOptions& options)
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
    const TMasterReadOptions& options)
{
    switch (options.ReadFrom) {
        case EMasterChannelKind::Leader:
            proto->set_read_from(NProto::TMasterReadOptions_EMasterReadKind_LEADER);
            break;
        case EMasterChannelKind::Follower:
            proto->set_read_from(NProto::TMasterReadOptions_EMasterReadKind_FOLLOWER);
            break;
        case EMasterChannelKind::Cache:
            proto->set_read_from(NProto::TMasterReadOptions_EMasterReadKind_CACHE);
            break;
    }
    proto->set_success_expiration_time(NYT::ToProto<i64>(options.ExpireAfterSuccessfulUpdateTime));
    proto->set_failure_expiration_time(NYT::ToProto<i64>(options.ExpireAfterFailedUpdateTime));
    proto->set_cache_sticky_group_size(options.CacheStickyGroupSize);
}

void ToProto(
    NProto::TMutatingOptions* proto,
    const TMutatingOptions& options)
{
    ToProto(proto->mutable_mutation_id(), options.GetOrGenerateMutationId());
    proto->set_retry(options.Retry);
}

void ToProto(
    NProto::TSuppressableAccessTrackingOptions* proto,
    const TSuppressableAccessTrackingOptions& options)
{
    proto->set_suppress_access_tracking(options.SuppressAccessTracking);
    proto->set_suppress_modification_tracking(options.SuppressModificationTracking);
}

void ToProto(
    NProto::TTabletRangeOptions* proto,
    const TTabletRangeOptions& options)
{
    if (options.FirstTabletIndex) {
        proto->set_first_tablet_index(*options.FirstTabletIndex);
    }
    if (options.LastTabletIndex) {
        proto->set_last_tablet_index(*options.LastTabletIndex);
    }
}

////////////////////////////////////////////////////////////////////////////////
// ROWSETS
////////////////////////////////////////////////////////////////////////////////

template <class TRow>
struct TRowsetTraits;

template <>
struct TRowsetTraits<TUnversionedRow>
{
    static constexpr NProto::ERowsetKind Kind = NProto::ERowsetKind::UNVERSIONED;
};

template <>
struct TRowsetTraits<TVersionedRow>
{
    static constexpr NProto::ERowsetKind Kind = NProto::ERowsetKind::VERSIONED;
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
    descriptor->set_rowset_kind(NProto::ERowsetKind::UNVERSIONED);
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
        columnDescriptor->set_name(column.Name);
        columnDescriptor->set_type(static_cast<int>(column.Type));
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
            columns[i].Name = descriptor.columns(i).name();
        }
        if (descriptor.columns(i).has_type()) {
            columns[i].Type = EValueType(descriptor.columns(i).type());
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
