#pragma once

#include <yt/core/misc/ref.h>

#include <yt/core/rpc/public.h>

#include <yt/client/api/admin.h>
#include <yt/client/api/client.h>

#include <yt/client/api/rpc_proxy/proto/api_service.pb.h>

namespace NYT {
namespace NApi {
namespace NRpcProxy {

////////////////////////////////////////////////////////////////////////////////

void SetTimeoutOptions(
    NRpc::TClientRequest& request,
    const NApi::TTimeoutOptions& options);

Y_NO_RETURN void ThrowUnimplemented(const TString& method);

////////////////////////////////////////////////////////////////////////////////

namespace NProto {

void ToProto(
    NProto::TTransactionalOptions* proto,
    const NApi::TTransactionalOptions& options);

void ToProto(
    NProto::TPrerequisiteOptions* proto,
    const NApi::TPrerequisiteOptions& options);

void ToProto(
    NProto::TMasterReadOptions* proto,
    const NApi::TMasterReadOptions& options);

void ToProto(
    NProto::TMutatingOptions* proto,
    const NApi::TMutatingOptions& options);

void ToProto(
    NProto::TSuppressableAccessTrackingOptions* proto,
    const NApi::TSuppressableAccessTrackingOptions& options);

void ToProto(
    NProto::TTabletRangeOptions* proto,
    const NApi::TTabletRangeOptions& options);

void ToProto(
    NProto::TGetFileFromCacheResult* proto,
    const NApi::TGetFileFromCacheResult& result);

void FromProto(
    NApi::TGetFileFromCacheResult* result,
    const NProto::TGetFileFromCacheResult& proto);

void ToProto(
    NProto::TPutFileToCacheResult* proto,
    const NApi::TPutFileToCacheResult& result);

void FromProto(
    NApi::TPutFileToCacheResult* result,
    const NProto::TPutFileToCacheResult& proto);

void ToProto(NProto::TColumnSchema* protoSchema, const NTableClient::TColumnSchema& schema);
void FromProto(NTableClient::TColumnSchema* schema, const NProto::TColumnSchema& protoSchema);

void ToProto(NProto::TTableSchema* protoSchema, const NTableClient::TTableSchema& schema);
void FromProto(NTableClient::TTableSchema* schema, const NProto::TTableSchema& protoSchema);

// Doesn't fill cell_config_version.
void ToProto(
    NProto::TTabletInfo* protoTabletInfo,
    const NTabletClient::TTabletInfo& tabletInfo);
// Doesn't fill TableId, UpdateTime and Owners.
void FromProto(
    NTabletClient::TTabletInfo* tabletInfo,
    const NProto::TTabletInfo& protoTabletInfo);

void ToProto(
    NProto::TTabletReadOptions* proto,
    const NApi::TTabletReadOptions& options);

} // namespace NProto
////////////////////////////////////////////////////////////////////////////////

void ValidateRowsetDescriptor(
    const NProto::TRowsetDescriptor& descriptor,
    int expectedVersion,
    NProto::ERowsetKind expectedKind);

std::vector<TSharedRef> SerializeRowset(
    const NTableClient::TNameTablePtr& nameTable,
    TRange<NTableClient::TUnversionedRow> rows,
    NProto::TRowsetDescriptor* descriptor);

template <class TRow>
std::vector<TSharedRef> SerializeRowset(
    const NTableClient::TTableSchema& schema,
    TRange<TRow> rows,
    NProto::TRowsetDescriptor* descriptor);

template <class TRow>
TIntrusivePtr<NApi::IRowset<TRow>> DeserializeRowset(
    const NProto::TRowsetDescriptor& descriptor,
    const TSharedRef& data);

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpcProxy
} // namespace NApi
} // namespace NYT
