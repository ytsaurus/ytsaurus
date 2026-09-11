#pragma once

#include "public.h"

#include <yt/yt/ytlib/object_client/public.h>

#include <yt/yt/ytlib/table_client/public.h>

#include <yt/yt/ytlib/table_client/proto/table_ypath.pb.h>

#include <yt/yt/ytlib/tablet_client/proto/master_tablet_service.pb.h>

#include <yt/yt/ytlib/transaction_client/public.h>

#include <yt/yt/core/ypath/public.h>

#include <concepts>

namespace NYT::NApi::NNative {

////////////////////////////////////////////////////////////////////////////////

template <class TRequest>
concept CTabletOperationRequest =
    std::same_as<TRequest, NTableClient::NProto::TReqMount> ||
    std::same_as<TRequest, NTableClient::NProto::TReqUnmount> ||
    std::same_as<TRequest, NTableClient::NProto::TReqRemount> ||
    std::same_as<TRequest, NTableClient::NProto::TReqFreeze> ||
    std::same_as<TRequest, NTableClient::NProto::TReqUnfreeze> ||
    std::same_as<TRequest, NTableClient::NProto::TReqReshard>;

////////////////////////////////////////////////////////////////////////////////

struct TTabletOperationTarget
{
    NTableClient::TTableId TableId;
    NObjectClient::TCellTag NativeCellTag;
    NObjectClient::TCellTag ExternalCellTag;
    NYPath::TYPath RequestPath;
    NYPath::TYPath FullPath;
};

namespace NTabletOperationDetail {

// TODO(danilalexeev): Unify tablet-operation protos to replace reconstruction with in-place target enrichment.

NTabletClient::NProto::TReqMount MakeMasterTabletOperationRequest(
    const NTableClient::NProto::TReqMount& request);

NTabletClient::NProto::TReqUnmount MakeMasterTabletOperationRequest(
    const NTableClient::NProto::TReqUnmount& request);

NTabletClient::NProto::TReqRemount MakeMasterTabletOperationRequest(
    const NTableClient::NProto::TReqRemount& request);

NTabletClient::NProto::TReqFreeze MakeMasterTabletOperationRequest(
    const NTableClient::NProto::TReqFreeze& request);

NTabletClient::NProto::TReqUnfreeze MakeMasterTabletOperationRequest(
    const NTableClient::NProto::TReqUnfreeze& request);

NTabletClient::NProto::TReqReshard MakeMasterTabletOperationRequest(
    const NTableClient::NProto::TReqReshard& request);

void DoExecuteTabletOperationViaMaster(
    const IClientPtr& client,
    const TTabletOperationTarget& target,
    TStringBuf action,
    NTransactionClient::TTransactionActionData actionData);

} // namespace NTabletOperationDetail

TTabletOperationTarget ResolveTabletOperationTarget(
    const IClientPtr& client,
    const NYPath::TYPath& path);

TTabletOperationTarget ResolveTabletOperationTarget(
    NObjectClient::TObjectServiceProxy& proxy,
    const NYPath::TYPath& path);

template <CTabletOperationRequest TRequest>
void ExecuteTabletOperationViaMaster(
    const IClientPtr& client,
    const TTabletOperationTarget& target,
    TStringBuf action,
    const TRequest& request);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative

#define TABLET_OPERATION_INL_H_
#include "tablet_operation-inl.h"
#undef TABLET_OPERATION_INL_H_
