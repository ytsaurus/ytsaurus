#pragma once

#include "public.h"
#include "transaction.h"

namespace NYT::NOrm::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

struct IAggregateQueryExecutor
{
    virtual TAggregateQueryResult ExecuteAggregateQuery(
        TObjectTypeValue type,
        const std::optional<TObjectFilter>& filter,
        const TAttributeAggregateExpressions& aggregators,
        const TAttributeGroupingExpressions& groupByExpressions,
        std::optional<bool> fetchFinalizingObjects) = 0;

    virtual ~IAggregateQueryExecutor() = default;
};

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IAggregateQueryExecutor> MakeAggregateQueryExecutor(TTransactionPtr transaction);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NObjects
