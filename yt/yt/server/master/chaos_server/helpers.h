#pragma once

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/client/chaos_client/replication_card.h>

#include <yt/yt/client/transaction_client/public.h>

#include <yt/yt/core/ytree/interned_attributes.h>

#include <yt/yt/core/yson/string.h>

#include <library/cpp/yt/error/error.h>

namespace NYT::NChaosServer {

////////////////////////////////////////////////////////////////////////////////

TErrorOr<int> GetMinimalTabletCount(std::vector<TErrorOr<int>> tabletCounts);

NTransactionClient::TTimestamp GetLastEraTimestamp(const NChaosClient::TReplicationCardPtr& card);

TFuture<NChaosClient::TReplicationCardPtr> GetReplicationCard(
    const NApi::NNative::IConnectionPtr& connection,
    NChaosClient::TReplicationCardId replicationCardId,
    const NChaosClient::TReplicationCardFetchOptions& options = {},
    bool bypassCache = true);

TFuture<NYson::TYsonString> GetReplicationCardAttribute(
    const NApi::NNative::IConnectionPtr& connection,
    NChaosClient::TReplicationCardId replicationCardId,
    NYTree::TInternedAttributeKey key);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosServer
