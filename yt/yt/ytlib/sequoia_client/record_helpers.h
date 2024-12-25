#pragma once

#include "public.h"

namespace NYT::NSequoiaClient {

////////////////////////////////////////////////////////////////////////////////

inline constexpr auto LinkTargetPathTombstone = "";
inline constexpr auto NodeTombstoneId = NCypressClient::NullObjectId;
inline constexpr auto ChildNodeTombstoneId = NCypressClient::NullObjectId;

////////////////////////////////////////////////////////////////////////////////

bool IsTombstone(const NRecords::TPathToNodeId& record);
bool IsTombstone(const NRecords::TPathFork& record);
bool IsTombstone(const NRecords::TNodeIdToPath& record);
bool IsTombstone(const NRecords::TNodeFork& record);
bool IsTombstone(const NRecords::TChildNode& record);
bool IsTombstone(const NRecords::TChildFork& record);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSequoiaClient
