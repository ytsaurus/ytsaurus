#pragma once

#include "public.h"

#include <yt/yt/core/ytree/public.h>

namespace NYT::NCrossClusterReplicatedState {

////////////////////////////////////////////////////////////////////////////////

using TReplicaVersion = std::pair<ui64, std::string>;

inline constexpr auto NullReplicaVersion = TReplicaVersion(0, {});

TReplicaVersion ExtractVersion(const NYTree::INodePtr& node);

void PutVersion(const NYTree::IMapNodePtr& node, const TReplicaVersion& version);

std::string MakeVersionAttributeValue(const TReplicaVersion& version);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCrossClusterReplicatedState
