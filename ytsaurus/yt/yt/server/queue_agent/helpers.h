#pragma once

#include "private.h"

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/client/object_client/public.h>

#include <yt/yt/client/api/public.h>

namespace NYT::NQueueAgent {

////////////////////////////////////////////////////////////////////////////////

TErrorOr<EQueueFamily> DeduceQueueFamily(const NQueueClient::TQueueTableRow& row);

bool IsReplicatedTableObjectType(NCypressClient::EObjectType type);
bool IsReplicatedTableObjectType(const std::optional<NCypressClient::EObjectType>& type);

////////////////////////////////////////////////////////////////////////////////

NApi::NNative::IClientPtr AssertNativeClient(const NApi::IClientPtr& client);

////////////////////////////////////////////////////////////////////////////////

THashMap<int, THashMap<i64, i64>> CollectCumulativeDataWeights(
    const NYPath::TYPath& path,
    NApi::IClientPtr client,
    const std::vector<std::pair<int, i64>>& tabletAndRowIndices,
    const NLogging::TLogger& logger);

////////////////////////////////////////////////////////////////////////////////

std::optional<i64> OptionalSub(const std::optional<i64> lhs, const std::optional<i64> rhs);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueueAgent
