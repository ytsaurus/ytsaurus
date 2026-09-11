#pragma once

#include "public.h"

#include <yt/yt/core/ypath/public.h>

#include <yt/yt/core/ytree/public.h>

#include <optional>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

struct TReplicaLocation
{
    std::string ClusterName;
    NYPath::TYPath Path;
};

//! Picks the enabled replica with the lexicographically smallest id from a Cypress @replicas map.
//! If |requiredContentType| is set, only replicas with that content_type are considered.
TReplicaLocation FindEnabledReplica(
    const NYTree::IMapNodePtr& replicas,
    const NYPath::TYPath& objectPath,
    std::optional<TStringBuf> requiredContentType = {});

//! Picks an enabled replica with mode=sync from a Cypress @replicas map.
//! If |requiredContentType| is set, only replicas with that content_type are considered.
TReplicaLocation FindEnabledSyncReplica(
    const NYTree::IMapNodePtr& replicas,
    const NYPath::TYPath& objectPath,
    std::optional<TStringBuf> requiredContentType = {});

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
