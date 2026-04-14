#pragma once

#include "private.h"

#include <yt/yt/client/api/client.h>

namespace NYT::NQueryTracker {

////////////////////////////////////////////////////////////////////////////////

struct TUpdateQueryOptions
{
    NYTree::IMapNodePtr NewAnnotations;
    std::optional<std::vector<std::string>> NewAccessControlObjects;
};

////////////////////////////////////////////////////////////////////////////////

struct ISearchIndex
    : public TRefCounted
{
    virtual void AddQuery(const NApi::TQuery& query, NApi::ITransactionPtr transaction) = 0;

    virtual void UpdateQuery(const NApi::TQuery& query, const TUpdateQueryOptions& options, NApi::ITransactionPtr transaction) = 0;

    virtual void RemoveQuery(const NApi::TQuery& query, NApi::ITransactionPtr transaction) = 0;

    virtual NApi::TListQueriesResult ListQueries(const NApi::TListQueriesOptions& options, const std::string& user) = 0;
};

DEFINE_REFCOUNTED_TYPE(ISearchIndex)

////////////////////////////////////////////////////////////////////////////////

ISearchIndexPtr CreateTimeBasedIndex(NApi::IClientPtr stateClient, NYPath::TYPath stateRoot);

ISearchIndexPtr CreateTokenBasedIndex(NApi::IClientPtr stateClient, NYPath::TYPath stateRoot);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryTracker
