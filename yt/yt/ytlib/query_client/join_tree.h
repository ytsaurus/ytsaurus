#pragma once

#include <yt/yt/library/query/base/query.h>

namespace NYT::NQueryClient {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(IJoinTreeNode)

struct IJoinTreeNode
    : public TRefCounted
{
    virtual TConstQueryPtr GetQuery() const = 0;

    virtual std::optional<NObjectClient::TObjectId> GetTableId() const = 0;

    virtual std::pair<IJoinTreeNodePtr, IJoinTreeNodePtr> GetChildren() const = 0;
};

DEFINE_REFCOUNTED_TYPE(IJoinTreeNode)

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(IJoinTree)

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
