#pragma once

#include <yt/core/misc/ref.h>

#include <yt/ytlib/api/public.h>

#include <yt/ytlib/rpc_proxy/api_service.pb.h>

namespace NYT {
namespace NRpcProxy {

////////////////////////////////////////////////////////////////////

void ValidateRowsetDescriptor(
    const NProto::TRowsetDescriptor& descriptor,
    int expectedVersion,
    NProto::ERowsetKind expectedKind);

template <class TRow>
std::vector<TSharedRef> SerializeRowset(
    const NTableClient::TNameTablePtr& nameTable,
    const TRange<TRow>& rows,
    NProto::TRowsetDescriptor* descriptor);

template <class TRow>
TIntrusivePtr<NApi::IRowset<TRow>> DeserializeRowset(
    const NProto::TRowsetDescriptor& descriptor,
    const TSharedRef& data);

////////////////////////////////////////////////////////////////////

} // namespace NRpcProxy
} // namespace NYT