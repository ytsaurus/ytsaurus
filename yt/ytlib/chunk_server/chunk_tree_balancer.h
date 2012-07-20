#pragma once

#include "public.h"

#include <ytlib/cell_master/public.h>
#include <ytlib/chunk_server/chunk_manager.pb.h>

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
        NProto::TMsgRebalanceChunkTree* message);

    bool RebalanceChunkTree(
        TChunkList* chunkList,
        const NProto::TMsgRebalanceChunkTree& message);

private:
    NCellMaster::TBootstrap* Bootstrap;
    TChunkTreeBalancerConfigPtr Config;

    void MergeChunkTrees(
        std::vector<TChunkTreeRef>* children,
        TChunkTreeRef child,
        const NProto::TMsgRebalanceChunkTree& message);
    
    void AppendChunkTree(
        std::vector<TChunkTreeRef>* children,
        TChunkTreeRef child,
        const NProto::TMsgRebalanceChunkTree& message);

    void InitRebalanceMessage(
        TChunkList* chunkList,
        NProto::TMsgRebalanceChunkTree* message);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
