#pragma once

#include "public.h"

#include <yt/yt/server/lib/tablet_balancer/table.h>

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/core/ytree/public.h>

namespace NYT::NTabletBalancer {

////////////////////////////////////////////////////////////////////////////////

//! Fetch attributes using CellTag from ObjectId.
THashMap<NObjectClient::TObjectId, NYTree::IAttributeDictionaryPtr> FetchAttributes(
    const NApi::NNative::IClientPtr& client,
    const THashSet<NObjectClient::TObjectId>& objectIds,
    const std::vector<TString>& attributeKeys);

THashMap<NObjectClient::TObjectId, NYTree::IAttributeDictionaryPtr> FetchTableAttributes(
    const NApi::NNative::IClientPtr& client,
    const THashSet<TTableId>& tableIds,
    const std::vector<TString>& attributeKeys,
    const THashMap<TTableId, TTablePtr>& Tables);

TInstant TruncatedNow();

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletBalancer
