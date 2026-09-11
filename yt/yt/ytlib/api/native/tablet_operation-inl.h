#ifndef TABLET_OPERATION_INL_H_
#error "Direct inclusion of this file is not allowed, include tablet_operation.h"
// For the sake of sane code completion.
#include "tablet_operation.h"
#endif

#include <yt/yt/ytlib/transaction_client/action.h>

#include <yt/yt/client/object_client/helpers.h>

namespace NYT::NApi::NNative {

////////////////////////////////////////////////////////////////////////////////

template <CTabletOperationRequest TRequest>
void ExecuteTabletOperationViaMaster(
    const IClientPtr& client,
    const TTabletOperationTarget& target,
    TStringBuf action,
    const TRequest& request)
{
    auto masterRequest = NTabletOperationDetail::MakeMasterTabletOperationRequest(request);
    ToProto(masterRequest.mutable_table_id(), target.TableId);
    if constexpr (std::same_as<TRequest, NTableClient::NProto::TReqMount>) {
        masterRequest.set_path(target.FullPath);
    }

    NTabletOperationDetail::DoExecuteTabletOperationViaMaster(
        client,
        target,
        action,
        NTransactionClient::MakeTransactionActionData(masterRequest));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative
