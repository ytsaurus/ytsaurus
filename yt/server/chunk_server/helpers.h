#pragma once

#include "public.h"

#include <core/yson/public.h>

#include <ytlib/new_table_client/public.h>

#include <server/cypress_server/public.h>

#include <server/security_server/cluster_resources.h>
#include <yt/ytlib/chunk_client/data_statistics.pb.h>

namespace NYT {
namespace NChunkServer {

////////////////////////////////////////////////////////////////////////////////

template <class F>
void VisitUniqueAncestors(TChunkList* chunkList, F functor);

template <class F>
void VisitAncestors(TChunkList* chunkList, F functor);

template <class F>
void AttachToChunkList(
    TChunkList* chunkList,
    TChunkTree** childrenBegin,
    TChunkTree** childrenEnd,
    F childAction);

template <class F>
void DetachFromChunkList(
    TChunkList* chunkList,
    TChunkTree** childrenBegin,
    TChunkTree** childrenEnd,
    F childAction);

void SetChunkTreeParent(TChunkList* parent, TChunkTree* child);
void ResetChunkTreeParent(TChunkList* parent, TChunkTree* child);

TChunkTreeStatistics GetChunkTreeStatistics(TChunkTree* chunkTree);
void AccumulateChildStatistics(
    TChunkList* chunkList,
    TChunkTree* child,
    TChunkTreeStatistics* statistics);
void ResetChunkListStatistics(TChunkList* chunkList);
void RecomputeChunkListStatistics(TChunkList* chunkList);

NSecurityServer::TClusterResources GetDiskUsage(
    const NChunkClient::NProto::TDataStatistics& statistics,
    int replicationFactor);

std::vector<TChunkOwnerBase*> GetOwningNodes(
    TChunkTree* chunkTree);
void SerializeOwningNodesPaths(
    NCypressServer::TCypressManagerPtr cypressManager,
    TChunkTree* chunkTree,
    NYson::IYsonConsumer* consumer);

void SerializeOwningNodesPaths(
    NCypressServer::TCypressManagerPtr cypressManager,
    TChunkTree* chunkTree,
    NYson::IYsonConsumer* consumer);

NVersionedTableClient::TOwningKey GetMaxKey(const TChunk* chunk);
NVersionedTableClient::TOwningKey GetMaxKey(const TChunkList* chunkList);
NVersionedTableClient::TOwningKey GetMaxKey(const TChunkTree* chunkTree);

NVersionedTableClient::TOwningKey GetMinKey(const TChunk* chunk);
NVersionedTableClient::TOwningKey GetMinKey(const TChunkList* chunkList);
NVersionedTableClient::TOwningKey GetMinKey(const TChunkTree* chunkTree);

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT

#define HELPERS_INL_H_
#include "helpers-inl.h"
#undef HELPERS_INL_H_
