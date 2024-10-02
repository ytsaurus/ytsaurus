#pragma once
#ifndef REQUEST_HANDLER_INL_H_
#error "Direct inclusion of this file is not allowed, include request_handler.h"
// For the sake of sane code completion.
#include "request_handler.h"
#endif

namespace NYT::NOrm::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

template <typename TRequestCommonOptions>
TCommonOptions GetCommonOptions(const TRequestCommonOptions& commonOptions)
{
    return {
        .AllowFullScan = AllowFullScanFromOptions(commonOptions),
        .CollectStatistics = commonOptions.fetch_performance_statistics()
    };
}

template <CCanBeDryRun TRequest>
void TRequestHandler::RunPrecommitActions(
    const NRpc::IServiceContextPtr& context,
    TTransactionId transactionId,
    TRequest&& request) const
{
    if constexpr (std::is_same_v<TRequest, TUpdateObjectsRequest>) {
        DoUpdateObjects(
            context,
            transactionId,
            std::move(request),
            /*dryRun*/ true);
    } else if constexpr (std::is_same_v<TRequest, TRemoveObjectsRequest>) {
        DoRemoveObjects(
            context,
            transactionId,
            std::move(request),
            /*dryRun*/ true);
    } else if constexpr (std::is_same_v<TRequest, TCreateObjectsRequest>) {
        DoCreateObjects(
            context,
            transactionId,
            std::move(request),
            /*dryRun*/ true);
    } else {
        static_assert(
            std::is_same_v<TRequest, TUpdateObjectsRequest>,
            "Have no implemetation for this request to dry run");
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NObjects
