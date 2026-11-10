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

template <class T>
concept CHasKey = requires(const T& t) { t.Key; };

template <CHasKey TRecord>
TStrongOrderingTags GetStrongOrderingTags(const TRecord& record);

// Acl stuff.
TStrongOrderingTags GetStrongOrderingTags(const NRecords::TAclsKey& key);

// Resolve stuff.
TStrongOrderingTags GetStrongOrderingTags(const NRecords::TChildNodeKey& key);
TStrongOrderingTags GetStrongOrderingTags(const NRecords::TNodeIdToPathKey& key);
TStrongOrderingTags GetStrongOrderingTags(const NRecords::TPathToNodeIdKey& key);

// Fork stuff.
TStrongOrderingTags GetStrongOrderingTags(const NRecords::TChildForkKey& key);
TStrongOrderingTags GetStrongOrderingTags(const NRecords::TNodeForkKey& key);
TStrongOrderingTags GetStrongOrderingTags(const NRecords::TNodeSnapshotKey& key);
TStrongOrderingTags GetStrongOrderingTags(const NRecords::TPathForkKey& key);

// Transaction stuff.
TStrongOrderingTags GetStrongOrderingTags(const NRecords::TDependentTransactionKey& key);
TStrongOrderingTags GetStrongOrderingTags(const NRecords::TDoomedTransactionKey& key);
TStrongOrderingTags GetStrongOrderingTags(const NRecords::TTransactionDescendantKey& key);
TStrongOrderingTags GetStrongOrderingTags(const NRecords::TTransactionReplicaKey& key);
TStrongOrderingTags GetStrongOrderingTags(const NRecords::TTransactionKey& key);

// Response keeper stuff.
TStrongOrderingTags GetStrongOrderingTags(const NRecords::TSequoiaResponseKeeperKey& key);

// Chunk replica stuff.
// NB: Chunk refresh queue has no key, so it has to be specified directly.
TStrongOrderingTags GetStrongOrderingTags(const NRecords::TChunkRefreshQueue& record);
TStrongOrderingTags GetStrongOrderingTags(const NRecords::TChunkReplicasKey& key);
TStrongOrderingTags GetStrongOrderingTags(const NRecords::TLocationReplicasKey& key);
TStrongOrderingTags GetStrongOrderingTags(const NRecords::TUnapprovedChunkReplicasKey& key);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSequoiaClient

#define RECORD_HELPERS_INL_H_
#include "record_helpers-inl.h"
#undef RECORD_HELPERS_INL_H_
