#pragma once

#include "public.h"

#include <yt/yt/ytlib/object_client/public.h>

#include <yt/yt/ytlib/table_client/public.h>

#include <yt/yt/ytlib/transaction_client/public.h>

#include <yt/yt/core/ypath/public.h>

namespace NYT::NApi::NNative {

////////////////////////////////////////////////////////////////////////////////

struct TTwoPhaseTableOperationTarget
{
    NTableClient::TTableId TableId;
    NObjectClient::TCellTag NativeCellTag;
    NObjectClient::TCellTag ExternalCellTag;
    NYPath::TYPath RequestPath;
    NYPath::TYPath FullPath;
};

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

////////////////////////////////////////////////////////////////////////////////

void DoExecuteTwoPhaseTableOperationViaMaster(
    const IClientPtr& client,
    const TTwoPhaseTableOperationTarget& target,
    TStringBuf action,
    NTransactionClient::TTransactionActionData actionData);

////////////////////////////////////////////////////////////////////////////////

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

TTwoPhaseTableOperationTarget ResolveTwoPhaseTableOperationTarget(
    const IClientPtr& client,
    const NYPath::TYPath& path);

TTwoPhaseTableOperationTarget ResolveTwoPhaseTableOperationTarget(
    NObjectClient::TObjectServiceProxy& proxy,
    const NYPath::TYPath& path);

template <CTwoPhaseTableRequest TRequest>
void ExecuteTwoPhaseTableOperationViaMaster(
    const IClientPtr& client,
    const TTwoPhaseTableOperationTarget& target,
    TStringBuf action,
    TRequest* request);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative

#define TWO_PHASE_TABLE_OPERATION_INL_H_
#include "two_phase_table_operation-inl.h"
#undef TWO_PHASE_TABLE_OPERATION_INL_H_
