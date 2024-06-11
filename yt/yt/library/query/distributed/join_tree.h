#pragma once

#include "public.h"

#include <yt/yt/library/query/base/query.h>

namespace NYT::NQueryClient {

////////////////////////////////////////////////////////////////////////////////

struct IJoinTreeNode
    : public TRefCounted
{
    virtual TConstQueryPtr GetQuery() const = 0;

    virtual NObjectClient::TObjectId GetTableId() const = 0;

    virtual std::pair<IJoinTreeNodePtr, IJoinTreeNodePtr> GetChildren() const = 0;
};

DEFINE_REFCOUNTED_TYPE(IJoinTreeNode)

////////////////////////////////////////////////////////////////////////////////

struct IJoinTree
    : public TRefCounted
{
    virtual IJoinTreeNodePtr GetRoot() const = 0;
};

DEFINE_REFCOUNTED_TYPE(IJoinTree)

////////////////////////////////////////////////////////////////////////////////

IJoinTreePtr MakeIndexJoinTree(const TConstQueryPtr &query, const TDataSource& primaryTable);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
