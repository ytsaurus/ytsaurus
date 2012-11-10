#pragma once

#include "public.h"

#include <server/cell_master/public.h>
#include <server/chunk_server/chunk_manager.pb.h>

namespace NYT {
namespace NChunkServer {

////////////////////////////////////////////////////////////////////////////////

class TChunkTreeBalancer
{
public:
    TChunkTreeBalancer(
        NCellMaster::TBootstrap* bootstrap,
        TChunkTreeBalancerConfigPtr config);

    bool CheckRebalanceNeeded(
        TChunkList* chunkList,
        NProto::TMetaReqRebalanceChunkTree* message);

    bool RebalanceChunkTree(
        TChunkList* chunkList,
        const NProto::TMetaReqRebalanceChunkTree& message);

private:
    NCellMaster::TBootstrap* Bootstrap;
    TChunkTreeBalancerConfigPtr Config;

    void MergeChunkTrees(
        std::vector<TChunkTreeRef>* children,
        TChunkTreeRef child,
        const NProto::TMetaReqRebalanceChunkTree& message);
    void AppendChunkTree(
        std::vector<TChunkTreeRef>* children,
        TChunkTreeRef child,
        const NProto::TMetaReqRebalanceChunkTree& message);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
