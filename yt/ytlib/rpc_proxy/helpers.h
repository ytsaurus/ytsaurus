#pragma once

#include <yt/core/misc/ref.h>

#include <yt/core/rpc/public.h>

#include <yt/ytlib/api/public.h>

#include <yt/ytlib/rpc_proxy/proto/api_service.pb.h>

namespace NYT {
namespace NRpcProxy {

////////////////////////////////////////////////////////////////////////////////

void SetTimeoutOptions(
    NRpc::TClientRequest& request,
    const NApi::TTimeoutOptions& options);

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

////////////////////////////////////////////////////////////////////////////////

void ValidateRowsetDescriptor(
    const NProto::TRowsetDescriptor& descriptor,
    int expectedVersion,
    NProto::ERowsetKind expectedKind);

std::vector<TSharedRef> SerializeRowset(
    const NTableClient::TNameTablePtr& nameTable,
    const TRange<NTableClient::TUnversionedRow>& rows,
    NProto::TRowsetDescriptor* descriptor);

template <class TRow>
std::vector<TSharedRef> SerializeRowset(
    const NTableClient::TTableSchema& schema,
    const TRange<TRow>& rows,
    NProto::TRowsetDescriptor* descriptor);

template <class TRow>
TIntrusivePtr<NApi::IRowset<TRow>> DeserializeRowset(
    const NProto::TRowsetDescriptor& descriptor,
    const TSharedRef& data);

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpcProxy
} // namespace NYT
