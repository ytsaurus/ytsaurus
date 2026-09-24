#ifndef TWO_PHASE_TABLE_OPERATION_INL_H_
#error "Direct inclusion of this file is not allowed, include two_phase_table_operation.h"
// For the sake of sane code completion.
#include "two_phase_table_operation.h"
#endif

#include <yt/yt/ytlib/transaction_client/action.h>

#include <yt/yt/client/object_client/helpers.h>

namespace NYT::NApi::NNative {

////////////////////////////////////////////////////////////////////////////////

template <CTwoPhaseTableRequest TRequest>
void ExecuteTwoPhaseTableOperationViaMaster(
    const IClientPtr& client,
    const TTwoPhaseTableOperationTarget& target,
    TStringBuf action,
    TRequest* request)
{
    ToProto(request->mutable_table_id(), target.TableId);
    if constexpr (std::same_as<TRequest, NTabletClient::NProto::TReqMount>) {
        request->set_path(target.FullPath);
    }

    NDetail::DoExecuteTwoPhaseTableOperationViaMaster(
        client,
        target,
        action,
        NTransactionClient::MakeTransactionActionData(*request));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative
