#pragma once

#include "public.h"
#include "transaction.h"

namespace NYT::NOrm::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(ENonexistentObjectPolicy,
    ((Fail)         (0))
    ((Skip)         (1))
    ((Ignore)       (2))
);

////////////////////////////////////////////////////////////////////////////////

struct TGetQueryOptions
{
    ENonexistentObjectPolicy NonexistentObjectPolicy = ENonexistentObjectPolicy::Fail;
    bool FetchValues = true;
    bool FetchTimestamps = false;
    bool FetchRootObject = false;
    bool CheckReadPermissions = false;
    bool ReadUncommittedChanges = false;
};

void FormatValue(
    TStringBuilderBase* builder,
    const TGetQueryOptions& options,
    TStringBuf format);

////////////////////////////////////////////////////////////////////////////////

struct IGetQueryExecutor
{
    virtual void ExecuteConsuming(IAttributeValuesConsumerGroup* consumerGroup) && = 0;

    virtual ~IGetQueryExecutor() = default;
};

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IGetQueryExecutor> MakeGetQueryExecutor(
    TTransactionPtr transaction,
    TObjectTypeValue type,
    const TAttributeSelector& selector,
    const std::vector<TKeyAttributeMatches>& matches,
    const TGetQueryOptions& options);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NObjects
